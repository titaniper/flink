package com.ecubelabs.streams.debezium.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumValueSchema {
    public String type;

    public boolean optional;

    public String name;

    public int version;
}