package com.ecubelabs.streams.debezium.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumValuePayloadSource {
    public String version;

    public String connector;

    public String name;

    @JsonProperty("ts_ms")
    public long tsMs;

    public String snapshot;

    public String db;

    public String sequence;

    public String table;

    @JsonProperty("server_id")
    public long serverId;

    public String gtid;

    public String file;

    public long pos;

    public int row;

    public long thread;

    public String query;
}