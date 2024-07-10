package com.ecubelabs.streams.debezium.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumValuePayloadAfterMetadata {
    public String channel;

    public String from;

    public String to;

    public Integer partition;

    public boolean enabled;
}
