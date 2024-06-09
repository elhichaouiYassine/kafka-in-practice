package com.example.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import com.example.kafka.producer.BatchingProducer;

public abstract class KafkaTestBase {

    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.5.2";
    private static final String BOOTSTRAP_SERVERS;

    static {
        String external = System.getProperty("kafka.bootstrap.servers");
        if (external != null && !external.isBlank()) {
            BOOTSTRAP_SERVERS = external;
        } else {
            if (System.getProperty("api.version") == null) {
                System.setProperty("api.version", "1.44");
            }
            KafkaContainer kafka = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE));
            kafka.start();
            BOOTSTRAP_SERVERS = kafka.getBootstrapServers();
        }
    }

    protected static String bootstrapServers() {
        return BOOTSTRAP_SERVERS;
    }

    protected static String uniqueName(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    protected static void createTopic(String topic, int partitions) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
    }

    protected static void produce(BatchingProducer producer, String topic, int from, int count) throws Exception {
        List<CompletableFuture<?>> acks = new ArrayList<>(count);
        for (int i = from; i < from + count; i++) {
            acks.add(producer.send(topic, "key-" + i, "msg-" + i));
        }
        producer.flush();
        CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);
    }

    @FunctionalInterface
    protected interface Condition {
        boolean isMet() throws Exception;
    }
    protected static void awaitTrue(String description, Duration timeout, Condition condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.isMet()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out after " + timeout.toSeconds() + "s waiting for: " + description);
            }
            Thread.sleep(50);
        }
    }

    protected static void awaitTrue(String description, Condition condition) throws Exception {
        awaitTrue(description, Duration.ofSeconds(30), condition);
    }
}
