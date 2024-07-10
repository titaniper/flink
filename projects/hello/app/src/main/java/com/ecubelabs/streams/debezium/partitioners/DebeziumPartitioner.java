package com.ecubelabs.streams.debezium.partitioners;

import com.ecubelabs.streams.debezium.models.DebeziumValue;
import com.ecubelabs.streams.debezium.models.DebeziumValuePayloadAfterMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class DebeziumPartitioner implements Partitioner {
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        ObjectMapper objectMapper = new ObjectMapper();
        int numPartitions = cluster.partitionsForTopic(topic).size();
        int partition = (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        try {
            DebeziumValue debeziumValue = objectMapper.readValue(new String(valueBytes, StandardCharsets.UTF_8), DebeziumValue.class);
            DebeziumValuePayloadAfterMetadata metadata = debeziumValue.payload.after.getMetadata().orElse(null);
            if (metadata != null && metadata.partition != null) {
                return Math.min(metadata.partition, numPartitions-1);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return partition;
    }

    @Override
    public void close() {
    }
}
