package com.ecubelabs.streams.debezium.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumValuePayload {
    public DebeziumValuePayloadAfter after;

    public DebeziumValuePayloadSource source;
}
