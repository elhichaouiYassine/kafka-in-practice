package com.example.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.kafka.admin.ConsumerLagMonitor;
import com.example.kafka.consumer.PollLoopConsumer;
import com.example.kafka.producer.BatchingProducer;

@Timeout(120)
class ConsumerLagTest extends KafkaTestBase {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLagTest.class);
    private static final PollLoopConsumer.Settings FREQUENT_COMMITS =
            PollLoopConsumer.Settings.defaults().withMaxPollRecords(10).withCommitIntervalMs(100);

    @Test
    void lagGrowsWhileProducerOutpacesConsumer_thenDrainsOnceConsumerCatchesUp() throws Exception {
        String topic = uniqueName("lag-topic");
        String group = uniqueName("lag-group");
        createTopic(topic, 3);

        AtomicLong perRecordDelayMs = new AtomicLong(25);

        try (BatchingProducer producer = new BatchingProducer(bootstrapServers(), BatchingProducer.Settings.throughputTuned());
             ConsumerLagMonitor lagMonitor = new ConsumerLagMonitor(bootstrapServers());
             PollLoopConsumer consumer = new PollLoopConsumer(bootstrapServers(), group, List.of(topic), FREQUENT_COMMITS,
                     record -> Thread.sleep(perRecordDelayMs.get()))) {

            consumer.start();
            awaitTrue("consumer owns all 3 partitions", () -> consumer.assignment().size() == 3);

            produce(producer, topic, 0, 300);
            long lagAfterBurst1 = lagMonitor.totalLag(group, topic);
            log.info("Lag after burst 1 (300 records produced): {} (consumer has processed {})",
                    lagAfterBurst1, consumer.processedCount());
            assertTrue(lagAfterBurst1 > 0, "a slow consumer must be behind a fast producer");
            assertTrue(consumer.processedCount() < 300, "consumer cannot have finished already");

            produce(producer, topic, 300, 300);
            long lagAfterBurst2 = lagMonitor.totalLag(group, topic);
            log.info("Lag after burst 2 (600 records produced): {} (consumer has processed {})",
                    lagAfterBurst2, consumer.processedCount());
            assertTrue(lagAfterBurst2 > lagAfterBurst1,
                    "lag keeps growing while production outpaces consumption (" + lagAfterBurst1 + " -> " + lagAfterBurst2 + ")");

            perRecordDelayMs.set(0);
            awaitTrue("lag drains to zero", Duration.ofSeconds(60), () -> lagMonitor.totalLag(group, topic) == 0);
            log.info("Lag drained to 0 once the consumer sped up");

            assertEquals(600, consumer.processedCount(), "every record processed exactly once");
        }
    }

    @Test
    void lagStaysAtZeroWhenConsumerKeepsUp() throws Exception {
        String topic = uniqueName("lag-topic");
        String group = uniqueName("lag-group");
        createTopic(topic, 3);

        try (BatchingProducer producer = new BatchingProducer(bootstrapServers(), BatchingProducer.Settings.throughputTuned());
             ConsumerLagMonitor lagMonitor = new ConsumerLagMonitor(bootstrapServers());
             PollLoopConsumer consumer = new PollLoopConsumer(bootstrapServers(), group, List.of(topic), FREQUENT_COMMITS,
                     record -> { /* instant handler */ })) {

            consumer.start();
            awaitTrue("consumer owns all 3 partitions", () -> consumer.assignment().size() == 3);

            for (int burst = 0; burst < 5; burst++) {
                produce(producer, topic, burst * 100, 100);
            }

            awaitTrue("lag returns to zero", () -> lagMonitor.totalLag(group, topic) == 0);
            assertEquals(500, consumer.processedCount());
        }
    }
}
