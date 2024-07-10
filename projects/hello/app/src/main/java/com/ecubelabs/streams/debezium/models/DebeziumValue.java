package com.ecubelabs.streams.debezium.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumValue {
    public DebeziumValueSchema schema;

    public DebeziumValuePayload payload;
}