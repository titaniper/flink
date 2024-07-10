package com.ecubelabs.streams.debezium.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumValuePayloadAfter {
    public long id;

    public String type;

    public long occurredAt;

    public String txId;

    public long createdAt;

    public long updatedAt;

    public String data;

    public String actorId;

    private String metadata;

    public Optional<DebeziumValuePayloadAfterMetadata> getMetadata() throws JsonProcessingException {
        return  Optional.ofNullable(this.metadata == null ? null : new ObjectMapper().readValue(this.metadata, DebeziumValuePayloadAfterMetadata.class));
    }
}

