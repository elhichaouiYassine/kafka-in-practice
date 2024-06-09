package com.example.kafka.admin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
public final class ConsumerLagMonitor implements AutoCloseable {

    private final AdminClient admin;

    public ConsumerLagMonitor(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.admin = AdminClient.create(props);
    }
    public Map<TopicPartition, Long> lagPerPartition(String groupId, String topic)
            throws ExecutionException, InterruptedException {

        Map<TopicPartition, OffsetAndMetadata> committed =
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

        List<TopicPartition> partitions = admin.describeTopics(List.of(topic)).allTopicNames().get()
                .get(topic).partitions().stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .toList();

        Map<TopicPartition, OffsetSpec> latestRequest = new HashMap<>();
        Map<TopicPartition, OffsetSpec> earliestRequest = new HashMap<>();
        for (TopicPartition partition : partitions) {
            latestRequest.put(partition, OffsetSpec.latest());
            earliestRequest.put(partition, OffsetSpec.earliest());
        }
        Map<TopicPartition, ListOffsetsResultInfo> latest = admin.listOffsets(latestRequest).all().get();
        Map<TopicPartition, ListOffsetsResultInfo> earliest = admin.listOffsets(earliestRequest).all().get();

        Map<TopicPartition, Long> lag = new HashMap<>();
        for (TopicPartition partition : partitions) {
            OffsetAndMetadata groupOffset = committed.get(partition);
            long consumed = groupOffset != null ? groupOffset.offset() : earliest.get(partition).offset();
            lag.put(partition, Math.max(0, latest.get(partition).offset() - consumed));
        }
        return lag;
    }
    public long totalLag(String groupId, String topic) throws ExecutionException, InterruptedException {
        return lagPerPartition(groupId, topic).values().stream().mapToLong(Long::longValue).sum();
    }

    @Override
    public void close() {
        admin.close();
    }
}
