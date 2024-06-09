package com.example.kafka.producer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BatchingProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BatchingProducer.class);
    public record Settings(int batchSizeBytes, int lingerMs, String compressionType) {
        public static Settings kafkaDefaults() {
            return new Settings(16_384, 0, "none");
        }
        public static Settings throughputTuned() {
            return new Settings(131_072, 20, "lz4");
        }
        public static Settings noBatching() {
            return new Settings(0, 0, "none");
        }
        public Settings withLingerMs(int value) {
            return new Settings(batchSizeBytes, value, compressionType);
        }

        public Settings withCompressionType(String value) {
            return new Settings(batchSizeBytes, lingerMs, value);
        }
    }
    public record Stats(long acked, long failed, double avgLatencyMs, double maxLatencyMs) { }

    private final KafkaProducer<String, String> producer;
    private final Settings settings;
    private final LongAdder acked = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder latencySumNanos = new LongAdder();
    private final LongAccumulator latencyMaxNanos = new LongAccumulator(Long::max, 0L);

    public BatchingProducer(String bootstrapServers, Settings settings) {
        this.settings = settings;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, settings.batchSizeBytes());
        props.put(ProducerConfig.LINGER_MS_CONFIG, settings.lingerMs());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, settings.compressionType());

        this.producer = new KafkaProducer<>(props);
        log.debug("Created producer with {}", settings);
    }
    public CompletableFuture<RecordMetadata> send(String topic, String key, String value) {
        CompletableFuture<RecordMetadata> result = new CompletableFuture<>();
        long startNanos = System.nanoTime();
        try {
            producer.send(new ProducerRecord<>(topic, key, value), (metadata, error) -> {
                if (error != null) {
                    failed.increment();
                    result.completeExceptionally(error);
                } else {
                    long latency = System.nanoTime() - startNanos;
                    latencySumNanos.add(latency);
                    latencyMaxNanos.accumulate(latency);
                    acked.increment();
                    result.complete(metadata);
                }
            });
        } catch (RuntimeException e) {
            failed.increment();
            result.completeExceptionally(e);
        }
        return result;
    }
    public void flush() {
        producer.flush();
    }

    public Settings settings() {
        return settings;
    }

    public Stats stats() {
        long ok = acked.sum();
        double avgMs = ok == 0 ? 0 : latencySumNanos.sum() / (double) ok / 1_000_000.0;
        return new Stats(ok, failed.sum(), avgMs, latencyMaxNanos.get() / 1_000_000.0);
    }

    public double metric(String name) {
        for (Map.Entry<MetricName, ? extends Metric> entry : producer.metrics().entrySet()) {
            MetricName metricName = entry.getKey();
            if (metricName.group().equals("producer-metrics") && metricName.name().equals(name)) {
                Object value = entry.getValue().metricValue();
                return value instanceof Number number ? number.doubleValue() : Double.NaN;
            }
        }
        return Double.NaN;
    }

    @Override
    public void close() {
        producer.close();
        Stats s = stats();
        log.debug("Producer closed: acked={}, failed={}, avgLatency={}ms", s.acked(), s.failed(), s.avgLatencyMs());
    }
}
