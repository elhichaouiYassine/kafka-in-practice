package com.example.kafka.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public final class PollLoopConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PollLoopConsumer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private static final long RETRY_BACKOFF_NANOS = Duration.ofMillis(200).toNanos();
    @FunctionalInterface
    public interface RecordHandler {
        void handle(ConsumerRecord<String, String> record) throws Exception;
    }
    public record Settings(int maxPollRecords, int sessionTimeoutMs, long commitIntervalMs, String assignmentStrategy) {

        public static Settings defaults() {
            return new Settings(500, 10_000, 1_000, CooperativeStickyAssignor.class.getName());
        }

        public Settings withMaxPollRecords(int value) {
            return new Settings(value, sessionTimeoutMs, commitIntervalMs, assignmentStrategy);
        }

        public Settings withCommitIntervalMs(long value) {
            return new Settings(maxPollRecords, sessionTimeoutMs, value, assignmentStrategy);
        }

        public Settings withAssignmentStrategy(String value) {
            return new Settings(maxPollRecords, sessionTimeoutMs, commitIntervalMs, value);
        }
    }
    public record RebalanceEvent(RebalanceEvent.Type type, Set<TopicPartition> partitions) {
        public enum Type { ASSIGNED, REVOKED, LOST }
    }

    private final String groupId;
    private final List<String> topics;
    private final Settings settings;
    private final RecordHandler handler;
    private final KafkaConsumer<String, String> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> pendingCommits = new HashMap<>();
    private long lastCommitNanos = System.nanoTime();
    private final Set<TopicPartition> assignment = ConcurrentHashMap.newKeySet();
    private final List<RebalanceEvent> rebalanceEvents = new CopyOnWriteArrayList<>();
    private final AtomicLong processed = new AtomicLong();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean running = new AtomicBoolean();
    private final CountDownLatch stopped = new CountDownLatch(1);

    public PollLoopConsumer(String bootstrapServers, String groupId, Collection<String> topics,
                            Settings settings, RecordHandler handler) {
        this.groupId = groupId;
        this.topics = List.copyOf(topics);
        this.settings = settings;
        this.handler = handler;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, settings.maxPollRecords());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, settings.sessionTimeoutMs());
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, settings.sessionTimeoutMs() / 3);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, settings.assignmentStrategy());

        this.consumer = new KafkaConsumer<>(props);
    }
    public PollLoopConsumer start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Consumer already started");
        }
        running.set(true);
        Thread thread = new Thread(this::runLoop, "poll-loop-" + groupId);
        thread.setDaemon(true);
        thread.start();
        return this;
    }
    public Set<TopicPartition> assignment() {
        return Set.copyOf(assignment);
    }
    public List<RebalanceEvent> rebalanceEvents() {
        return List.copyOf(rebalanceEvents);
    }
    public long processedCount() {
        return processed.get();
    }

    @Override
    public void close() {
        if (!started.get()) {
            consumer.close();
            return;
        }
        if (running.compareAndSet(true, false)) {
            consumer.wakeup();
        }
        try {
            if (!stopped.await(30, TimeUnit.SECONDS)) {
                log.warn("[{}] Poll loop did not stop within 30 s", groupId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    private void runLoop() {
        try {
            consumer.subscribe(topics, rebalanceListener);
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                processBatch(records);
                commitIfDue();
            }
        } catch (WakeupException e) {
            log.debug("[{}] Woken up for shutdown", groupId);
        } catch (RuntimeException e) {
            log.error("[{}] Poll loop crashed", groupId, e);
        } finally {
            running.set(false);
            try {
                commitPending();
            } finally {
                try {
                    consumer.close();
                } catch (RuntimeException e) {
                    log.warn("[{}] Error while closing consumer", groupId, e);
                }
                stopped.countDown();
            }
        }
    }

    private void processBatch(ConsumerRecords<String, String> records) {
        boolean anyFailure = false;
        for (TopicPartition partition : records.partitions()) {
            for (ConsumerRecord<String, String> record : records.records(partition)) {
                if (!running.get()) {
                    return;
                }
                try {
                    handler.handle(record);
                } catch (Exception e) {
                    log.warn("[{}] Handler failed at {}@{}; retrying from there", groupId, partition, record.offset(), e);
                    consumer.seek(partition, record.offset());
                    anyFailure = true;
                    break;
                }
                pendingCommits.put(partition, new OffsetAndMetadata(record.offset() + 1));
                processed.incrementAndGet();
            }
        }
        if (anyFailure) {
            LockSupport.parkNanos(RETRY_BACKOFF_NANOS);
        }
    }

    private void commitIfDue() {
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastCommitNanos);
        if (!pendingCommits.isEmpty() && elapsedMs >= settings.commitIntervalMs()) {
            commitPending();
        }
    }

    private void commitPending() {
        if (pendingCommits.isEmpty()) {
            return;
        }
        try {
            try {
                consumer.commitSync(pendingCommits);
            } catch (WakeupException e) {
                consumer.commitSync(pendingCommits);
            }
            log.debug("[{}] Committed {}", groupId, pendingCommits);
        } catch (KafkaException e) {
            log.warn("[{}] Offset commit failed: {}", groupId, e.toString());
        } finally {
            pendingCommits.clear();
            lastCommitNanos = System.nanoTime();
        }
    }

    private final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            commitPending();
            assignment.removeAll(partitions);
            rebalanceEvents.add(new RebalanceEvent(RebalanceEvent.Type.REVOKED, Set.copyOf(partitions)));
            log.info("[{}] Revoked  {}", groupId, partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            assignment.addAll(partitions);
            rebalanceEvents.add(new RebalanceEvent(RebalanceEvent.Type.ASSIGNED, Set.copyOf(partitions)));
            log.info("[{}] Assigned {}", groupId, partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            pendingCommits.keySet().removeAll(partitions);
            assignment.removeAll(partitions);
            rebalanceEvents.add(new RebalanceEvent(RebalanceEvent.Type.LOST, Set.copyOf(partitions)));
            log.warn("[{}] Lost     {}", groupId, partitions);
        }
    };
}
