package com.example.kafka;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.kafka.producer.BatchingProducer;
import com.example.kafka.producer.BatchingProducer.Settings;

@Timeout(180)
class BatchingLingerTest extends KafkaTestBase {

    private static final Logger log = LoggerFactory.getLogger(BatchingLingerTest.class);
    private static final String PAYLOAD = "miaw miaw miaw miaw ".repeat(8);
    private record Run(double recordsPerSecond, double recordsPerRequest, double requests, double bytesOnWire,
                       BatchingProducer.Stats stats) {
        @Override
        public String toString() {
            return String.format("%,.0f records/s | %.1f records/request | %,.0f requests | %,.0f KiB on the wire | avg ack latency %.1f ms",
                    recordsPerSecond, recordsPerRequest, requests, bytesOnWire / 1024, stats.avgLatencyMs());
        }
    }

    @Test
    void lingerMsLetsAModerateRateStreamFormBatches() throws Exception {
        long onePerMillisecond = TimeUnit.MILLISECONDS.toNanos(1);

        Run noLinger = run(Settings.kafkaDefaults().withLingerMs(0), 1_500, onePerMillisecond);
        Run linger20 = run(Settings.kafkaDefaults().withLingerMs(20), 1_500, onePerMillisecond);

        log.info("linger.ms=0  -> {}", noLinger);
        log.info("linger.ms=20 -> {}", linger20);

        assertTrue(linger20.recordsPerRequest() > 2 * noLinger.recordsPerRequest(),
                "linger.ms=20 should at least double records per request");
        assertTrue(linger20.requests() < noLinger.requests(), "...and therefore need fewer requests");
    }

    @Test
    void lingerMsTradesLatencyForBatchingOnSparseTraffic() throws Exception {
        double noLingerMs = averageSingleRecordLatencyMs(Settings.kafkaDefaults().withLingerMs(0));
        double lingerMs = averageSingleRecordLatencyMs(Settings.kafkaDefaults().withLingerMs(300));

        log.info("Single record, linger.ms=0:   {} ms", String.format("%.1f", noLingerMs));
        log.info("Single record, linger.ms=300: {} ms", String.format("%.1f", lingerMs));

        assertTrue(lingerMs >= 290, "a lone record waits for the whole linger period, was " + lingerMs + " ms");
        assertTrue(noLingerMs < lingerMs, "without linger it is sent immediately");
    }

    @Test
    void aFullBatchIsSentImmediatelyEvenWithAVeryLongLinger() throws Exception {
        Settings tinyBatchesLongLinger = new Settings(1_024, 10_000, "none");
        String topic = uniqueName("batching-topic");
        createTopic(topic, 1);

        try (BatchingProducer producer = new BatchingProducer(bootstrapServers(), tinyBatchesLongLinger)) {
            List<CompletableFuture<?>> acks = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                acks.add(producer.send(topic, null, PAYLOAD + i));
            }

            long start = System.nanoTime();
            acks.get(0).get(5, TimeUnit.SECONDS);
            long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            log.info("First record acked after {} ms although linger.ms=10000", waitedMs);
            assertTrue(waitedMs < 5_000);
        }
    }

    @Test
    void batchingBoostsThroughputAndLatencyUnderLoad() throws Exception {
        int records = 10_000;

        Run noBatching = run(Settings.noBatching(), records, 0);
        Run tuned = run(Settings.throughputTuned(), records, 0);

        log.info("batching off   -> {}", noBatching);
        log.info("batching tuned -> {}", tuned);
        log.info("Speed-up: {}x", String.format("%.1f", tuned.recordsPerSecond() / noBatching.recordsPerSecond()));

        assertTrue(noBatching.recordsPerRequest() < 1.5, "without batching each record is its own request");
        assertTrue(tuned.recordsPerRequest() > 10 * noBatching.recordsPerRequest(), "tuned batches are far bigger");
        assertTrue(tuned.requests() < noBatching.requests() / 10, "so we need a fraction of the requests");
        assertTrue(tuned.recordsPerSecond() > noBatching.recordsPerSecond(), "higher throughput");
        assertTrue(tuned.stats().avgLatencyMs() < noBatching.stats().avgLatencyMs(), "lower average latency under load");
    }

    @Test
    void compressionShrinksTheBytesSentOverTheNetwork() throws Exception {
        Settings tuned = Settings.throughputTuned();
        Run none = run(tuned.withCompressionType("none"), 5_000, 0);
        Run lz4 = run(tuned.withCompressionType("lz4"), 5_000, 0);
        Run gzip = run(tuned.withCompressionType("gzip"), 5_000, 0);

        log.info("compression none -> {}", none);
        log.info("compression lz4  -> {}", lz4);
        log.info("compression gzip -> {}", gzip);

        assertTrue(lz4.bytesOnWire() < none.bytesOnWire() * 0.5, "lz4 should at least halve the traffic");
        assertTrue(gzip.bytesOnWire() < none.bytesOnWire() * 0.5, "gzip should at least halve the traffic");
    }

    private Run run(Settings settings, int count, long paceNanos) throws Exception {
        String topic = uniqueName("batching-topic");
        createTopic(topic, 1);

        try (BatchingProducer producer = new BatchingProducer(bootstrapServers(), settings)) {
            producer.send(topic, null, "warm-up").get(30, TimeUnit.SECONDS);
            double requestsBefore = producer.metric("request-total");
            double recordsBefore = producer.metric("record-send-total");
            double bytesBefore = producer.metric("outgoing-byte-total");

            long start = System.nanoTime();
            List<CompletableFuture<?>> acks = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                acks.add(producer.send(topic, null, PAYLOAD + i));
                if (paceNanos > 0) {
                    LockSupport.parkNanos(paceNanos);
                }
            }
            CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)).get(120, TimeUnit.SECONDS);
            double seconds = (System.nanoTime() - start) / 1e9;

            double requests = producer.metric("request-total") - requestsBefore;
            double records = producer.metric("record-send-total") - recordsBefore;
            double bytes = producer.metric("outgoing-byte-total") - bytesBefore;

            assertTrue(records >= count, "the producer metrics must have seen every record");
            return new Run(count / seconds, records / requests, requests, bytes, producer.stats());
        }
    }
    private double averageSingleRecordLatencyMs(Settings settings) throws Exception {
        String topic = uniqueName("batching-topic");
        createTopic(topic, 1);

        try (BatchingProducer producer = new BatchingProducer(bootstrapServers(), settings)) {
            producer.send(topic, null, "warm-up").get(30, TimeUnit.SECONDS);

            int samples = 5;
            long totalNanos = 0;
            for (int i = 0; i < samples; i++) {
                long start = System.nanoTime();
                producer.send(topic, null, PAYLOAD + i).get(30, TimeUnit.SECONDS);
                totalNanos += System.nanoTime() - start;
            }
            return totalNanos / (double) samples / 1_000_000.0;
        }
    }
}
