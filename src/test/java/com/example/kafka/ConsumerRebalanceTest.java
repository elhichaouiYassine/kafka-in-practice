package com.example.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.example.kafka.consumer.PollLoopConsumer;
import com.example.kafka.consumer.PollLoopConsumer.RebalanceEvent;
import com.example.kafka.producer.BatchingProducer;

@Timeout(120)
class ConsumerRebalanceTest extends KafkaTestBase {

    private static final int PARTITIONS = 4;
    private static final String COOPERATIVE = CooperativeStickyAssignor.class.getName();
    private static final String EAGER = RangeAssignor.class.getName();

    @Test
    void partitionsAreSplitWhenAConsumerJoinsAndReclaimedWhenItLeaves() throws Exception {
        String topic = uniqueName("rebalance-topic");
        String group = uniqueName("rebalance-group");
        createTopic(topic, PARTITIONS);

        try (PollLoopConsumer first = newConsumer(group, topic, COOPERATIVE, r -> { });
             PollLoopConsumer second = newConsumer(group, topic, COOPERATIVE, r -> { })) {

            first.start();
            awaitTrue("first consumer owns every partition", () -> first.assignment().size() == PARTITIONS);

            second.start();
            awaitTrue("partitions split 2/2",
                    () -> first.assignment().size() == 2 && second.assignment().size() == 2);

            Set<TopicPartition> union = new HashSet<>(first.assignment());
            union.addAll(second.assignment());
            assertEquals(PARTITIONS, union.size(), "a partition must never be owned by two consumers at once");

            second.close();
            awaitTrue("first consumer takes everything back", () -> first.assignment().size() == PARTITIONS);
        }
    }

    @Test
    void cooperativeRebalanceOnlyRevokesThePartitionsThatMove() throws Exception {
        int revoked = partitionsRevokedFromFirstConsumerWhenSecondJoins(COOPERATIVE);
        assertEquals(PARTITIONS / 2, revoked);
    }

    @Test
    void eagerRebalanceRevokesEveryPartitionBeforeReassigning() throws Exception {
        int revoked = partitionsRevokedFromFirstConsumerWhenSecondJoins(EAGER);
        assertEquals(PARTITIONS, revoked);
    }

    @Test
    void noRecordIsLostOrReprocessedWhileConsumersJoinAndLeave() throws Exception {
        String topic = uniqueName("rebalance-topic");
        String group = uniqueName("rebalance-group");
        createTopic(topic, PARTITIONS);

        int total = 3_000;
        try (BatchingProducer producer = new BatchingProducer(bootstrapServers(), BatchingProducer.Settings.throughputTuned())) {
            produce(producer, topic, 0, total);
        }

        Map<String, AtomicInteger> timesSeen = new ConcurrentHashMap<>();
        PollLoopConsumer.RecordHandler handler = record -> {
            timesSeen.computeIfAbsent(record.value(), v -> new AtomicInteger()).incrementAndGet();
            Thread.sleep(2);
        };

        try (PollLoopConsumer first = newConsumer(group, topic, COOPERATIVE, handler);
             PollLoopConsumer second = newConsumer(group, topic, COOPERATIVE, handler)) {

            first.start();
            awaitTrue("first consumer is busy", () -> first.processedCount() >= 200);

            second.start();
            awaitTrue("second consumer received partitions and is working", () -> second.processedCount() > 0);

            second.close();
            awaitTrue("every record was handled", Duration.ofSeconds(90), () -> timesSeen.size() == total);
        }

        long reprocessed = timesSeen.values().stream().filter(count -> count.get() > 1).count();
        assertEquals(total, timesSeen.size(), "no record may be lost");
        assertEquals(0, reprocessed, "graceful rebalances must not cause re-processing");
    }

    private int partitionsRevokedFromFirstConsumerWhenSecondJoins(String assignor) throws Exception {
        String topic = uniqueName("rebalance-topic");
        String group = uniqueName("rebalance-group");
        createTopic(topic, PARTITIONS);

        try (PollLoopConsumer first = newConsumer(group, topic, assignor, r -> { });
             PollLoopConsumer second = newConsumer(group, topic, assignor, r -> { })) {

            first.start();
            awaitTrue("first consumer owns every partition", () -> first.assignment().size() == PARTITIONS);
            int eventsBeforeJoin = first.rebalanceEvents().size();

            second.start();
            awaitTrue("partitions split 2/2",
                    () -> first.assignment().size() == 2 && second.assignment().size() == 2);

            List<RebalanceEvent> allEvents = first.rebalanceEvents();
            int revoked = allEvents.subList(eventsBeforeJoin, allEvents.size()).stream()
                    .filter(event -> event.type() == RebalanceEvent.Type.REVOKED)
                    .mapToInt(event -> event.partitions().size())
                    .sum();
            assertTrue(revoked > 0, "the first consumer has to give something up for the second one");
            return revoked;
        }
    }

    private static PollLoopConsumer newConsumer(String group, String topic, String assignor, PollLoopConsumer.RecordHandler handler) {
        PollLoopConsumer.Settings settings = PollLoopConsumer.Settings.defaults()
                .withAssignmentStrategy(assignor)
                .withMaxPollRecords(50)
                .withCommitIntervalMs(200);
        return new PollLoopConsumer(bootstrapServers(), group, List.of(topic), settings, handler);
    }
}
