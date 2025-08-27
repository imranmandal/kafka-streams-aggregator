package com.kazam_dlg_ingest;

import com.kazam_dlg_ingest.models.ItemsModel;
import com.kazam_dlg_ingest.serder.JsonNodeSerde;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
        // stream.peek((key, value)->{
        // System.out.println(key + " " + value);
        // });
        // KTable<String, Long> aggregated = stream
        // .mapValues(value -> {
        // try {
        // JsonNode json = mapper.readTree(value);
        // String item = json.get("item").asText();
        // int count = json.get("count").asInt();
        // String timestamp = json.get("timestamp").asText();
        // System.out.println(
        // "RECEIVED PACKET: item=" + item + ", count=" + count + ", time - " +
        // timestamp);
        // // return json.get("item").asText() + "_" + json.get("org").asText()
        // // + ":" + json.get("count").asInt();
        // String key = json.get("item").asText() + "_" + json.get("org").asText();
        // JsonNode packet = new ItemsModel(json).toJsonNode();
        // return KeyValue.pair(key, packet);
        // } catch (Exception e) {
        // return KeyValue.pair("error", 0);
        // }
        // })
        // .filter((key, value) -> !key.startsWith("error"))
        // .map((key, value) -> {
        // String[] parts = value.split(":");
        // System.out.println("part " + parts[0]);
        // return KeyValue.pair(parts[0], Long.parseLong(parts[1]));
        // })
        // // .groupByKey()
        // // .reduce(Long::sum);
        // .groupBy((k, v) -> k, Grouped.with(Serdes.String(), Serdes.Long()))
        // .reduce(Long::sum, Materialized.with(Serdes.String(), Serdes.Long()));

        KTable<String, JsonNode> aggregated = stream
                .map((key, value) -> {
                    try {
                        JsonNode json = mapper.readTree(value);
                        String item = json.get("item").asText();
                        String org = json.get("org").asText();
                        String customKey = item + "_" + org;

                        JsonNode node = new ItemsModel(json).toJsonNode();
                        return KeyValue.pair(customKey, node); // ✅ Now key is String, value is JsonNode
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter((key, value) -> value != null)
                .groupByKey(Grouped.with(Serdes.String(), new JsonNodeSerde()))
                .reduce((v1, v2) -> {
                    int count1 = v1.get("count").asInt();
                    int count2 = v2.get("count").asInt();
                    ObjectNode updated = v1.deepCopy();
                    updated.put("count", count1 + count2);
                    return updated;
                });

        aggregated
                .toStream()
                .foreach((key, value) -> {
                    Instant currentTimestamp = Instant.now();
                    System.out.println("AGGREGATED >> " + key + " = " + value + ", at " +
                            currentTimestamp);
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
            System.err.println("Stream error 2: " + e.getMessage());
        });
        streams.start();
        System.out.println("AggregatorApp started. Waiting for messages...");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
