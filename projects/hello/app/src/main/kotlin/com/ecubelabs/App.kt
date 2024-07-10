package com.ecubelabs

import org.apache.flink.api.common.functions.FlatMapFunction
import java.util.*

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val properties = Properties().apply {
        setProperty("bootstrap.servers", "localhost:9092")
        setProperty("group.id", "flink-consumer")
    }

    val kafkaSource = FlinkKafkaConsumer<String>(
            "input-topic",
            SimpleStringSchema(),
            properties
    )

    val kafkaSink = FlinkKafkaProducer<String>(
            "output-topic",
            SimpleStringSchema(),
            properties
    )

    val text: DataStream<String> = env.addSource(kafkaSource)

    val wordCounts: DataStream<Tuple2<String, Int>> = text
            .flatMap(object : FlatMapFunction<String, Tuple2<String, Int>> {
                override fun flatMap(value: String, out: Collector<Tuple2<String, Int>>) {
                    value.split("\\s+".toRegex()).forEach { word ->
                        out.collect(Tuple2(word, 1))
                    }
                }
            })
            .returns(TypeInformation.of(object : TypeHint<Tuple2<String, Int>>() {}))
            .keyBy { it.f0 }
            .sum(1)

    val output = wordCounts.map { it.f0 + ": " + it.f1 }
            .returns(TypeInformation.of(String::class.java))

    output.addSink(kafkaSink)

    env.execute("Kafka Word Count Example")
}
