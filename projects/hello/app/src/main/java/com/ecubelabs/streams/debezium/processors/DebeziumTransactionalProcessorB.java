

package com.ecubelabs.streams.debezium.processors;

import com.ecubelabs.streams.debezium.models.DebeziumValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.To;

public class DebeziumTransactionalProcessorB extends AbstractProcessor<String, String> {
    @Override
    public void process(String key, String value) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            DebeziumValue debeziumValue = objectMapper.readValue(value, DebeziumValue.class);
            debeziumValue.payload.after.getMetadata().ifPresent(metadata -> {
                context.forward(key, value, To.child(metadata.to));
            });
            throw new RuntimeException("HI");
        } catch (Exception e) {}
    }

    @Override
    public void close() {
    }
}