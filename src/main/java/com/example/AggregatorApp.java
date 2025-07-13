package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.util.Properties;
import java.time.Instant;

public class AggregatorApp {

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregator-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        // raw-items topic
        KStream<String, String> stream = builder.stream("raw-items");

        KTable<String, Long> aggregated = stream
                .mapValues(value -> {
                    try {
                        JsonNode json = mapper.readTree(value);
                        String item = json.get("item").asText();
                        int count = json.get("count").asInt();
                        System.out.println("RECEIVED PACKET: item=" + item + ", count=" + count);
                        return json.get("item").asText() + "_" + json.get("org").asText()
                                + ":" + json.get("count").asInt();
                    } catch (Exception e) {
                        return "error:0";
                    }
                })
                .filter((key, value) -> !value.startsWith("error"))
                .map((key, value) -> {
                    String[] parts = value.split(":");
                    System.out.println("part " + parts[0]);
                    return KeyValue.pair(parts[0], Long.parseLong(parts[1]));
                })
                // .groupByKey()
                // .reduce(Long::sum);
                .groupBy((k, v) -> k, Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(Long::sum, Materialized.with(Serdes.String(), Serdes.Long()));

        aggregated
                .toStream()
                .foreach((key, value) -> {
                    Instant currentTimestamp = Instant.now();
                    System.out.println("AGGREGATED >> " + key + " = " + value + ", at " + currentTimestamp);
                });

        // aggregated
        // .toStream()
        // .mapValues((key, value) -> {
        // String data = "{\"item\":\"" + key + "\",\"totalCount\":" + value + "}";
        // System.out.println(data);
        // return data;
        // })
        // .to("aggregated-items", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler((t, e) -> {
            System.err.println("Stream error: " + e.getMessage());
        });
        streams.start();
        System.out.println("AggregatorApp started. Waiting for messages...");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
