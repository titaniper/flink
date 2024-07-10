package com.ecubelabs.configs;

public class KafkaStreamAppConfig {
    private String name;

    private String[] brokers;

    private String[] sourceTopics;

    private String[] sinkTopics;

    public KafkaStreamAppConfig(String name, String[] brokers, String[] sourceTopics, String[] sinkTopics) {
        this.name = name;
        this.brokers = brokers;
        this.sourceTopics = sourceTopics;
        this.sinkTopics = sinkTopics;
    }

    public String getName() {
        return name;
    }

    public String[] getBrokers() {
        return brokers;
    }

    public String[] getSinkTopics() {
        return sinkTopics;
    }

    public String[] getSourceTopics() {
        return sourceTopics;
    }
}
