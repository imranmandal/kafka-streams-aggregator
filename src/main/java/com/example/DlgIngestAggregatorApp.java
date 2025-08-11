package com.example;

import java.util.Properties;

import com.example.Aggregators.LogAggregator;
import com.example.models.EnergyParameterModels.EnergyParamBaseModel;
import com.example.utils.EventTimeExtractor;
import com.example.utils.TimeBoundaryUtil.TimeBoundary;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

public class DlgIngestAggregatorApp {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String args[]) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregator-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeExtractor.class);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, EnergyParamBaseModel> stream = builder.stream("energy-parameter-test-2",
                Consumed.with(Serdes.String(), Serdes.String()))
                .map((key, value) -> {
                    try {
                        JsonNode json = mapper.readTree(value);
                        EnergyParamBaseModel energyParam = new EnergyParamBaseModel(json).getData();

                        return KeyValue.pair(key, energyParam);
                    } catch (Exception e) {
                        System.err.println("DlgAggErr " + e.getMessage());
                        return null;
                    }
                })
                .filter((key, value) -> value != null);

        // **********
        // ----------
        // ----- Daily agg log
        // ----------
        // **********

        // new LogAggregator(stream, TimeBoundary.DAY,
        // "energy-param-daily-agg-store-test-2",
        // "energy-param-daily-agg-test-2").aggregate();

        // KStream<String, EnergyParamBaseModel> dailyAggStream = stream
        // .map((key, energyParam) -> {
        // try {
        // int utcOffsetSeconds = (energyParam.utc_offset != null ?
        // energyParam.utc_offset : 0) * 60;
        // String streamKey = new StreamKeyUtil(energyParam.comb_id, energyParam.org,
        // energyParam.timestamp,
        // utcOffsetSeconds, TimeBoundary.DAY)
        // .getStreamKey();

        // return KeyValue.pair(streamKey, energyParam);
        // } catch (Exception e) {
        // System.err.println("DlgAggErr " + e.getMessage());
        // return null;
        // }
        // })
        // .filter((key, value) -> value != null);

        // KTable<String, EnergyParamAgg> dailyAggregated = dailyAggStream
        // .groupByKey(Grouped.with("dailyAggregated", Serdes.String(), new
        // EnergyParamSerde()))
        // .aggregate(
        // () -> new EnergyParamAgg(null, null).getDefaultValue(),
        // (aggKey, newValue, aggValue) -> {
        // try {
        // aggValue = new EnergyParamAgg(aggValue, newValue).getData();
        // } catch (Exception e) {
        // System.err.println("DlgAggError " + e.getMessage());
        // }
        // return aggValue;
        // },
        // Materialized
        // .<String, EnergyParamAgg, KeyValueStore<Bytes,
        // byte[]>>as("energy-param-daily-agg-9")
        // .withKeySerde(Serdes.String())
        // .withValueSerde(new EnergyParamAggSerde()));

        // dailyAggregated
        // .toStream()
        // .map((windowedKey, value) -> {
        // String keyname = windowedKey;
        // StreamKeyUtil key = new StreamKeyUtil("", "", 0, 0,
        // TimeBoundary.UNKNOWN).parseStreamKey(keyname);
        // value.setTimestamp(key.boundaries.startTime, key.boundaries.endTime);

        // System.out.println(
        // "Daily - " + "\nKey: " + key + "\nStart: " + key.boundaries.startTime + "
        // End: "
        // + key.boundaries.endTime + "\nValue:");
        // System.out.print(value.toJsonNode());
        // System.out.println("");

        // return KeyValue.pair(windowedKey, value);
        // })
        // .to("energy-parameter-daily", Produced.with(Serdes.String(), new
        // EnergyParamAggSerde()));

        // ----- Daily agg log end
        // ----------
        // **********

        // **********
        // ----------
        // ----- Hourly agg log
        // ----------
        // **********

        new LogAggregator(stream, TimeBoundary.HOUR,
                "energy-param-hourly-agg-store-test-2",
                "energy-param-hourly-agg-test-2").aggregate();

        // KStream<String, EnergyParamBaseModel> hourlyAggStream = stream
        // .map((key, energyParam) -> {
        // try {
        // int utcOffsetSeconds = (energyParam.utc_offset != null ?
        // energyParam.utc_offset : 0) * 60;
        // String streamKey = new StreamKeyUtil(energyParam.comb_id, energyParam.org,
        // energyParam.timestamp,
        // utcOffsetSeconds, TimeBoundary.DAY)
        // .getStreamKey();

        // return KeyValue.pair(streamKey, energyParam);
        // } catch (Exception e) {
        // System.err.println("DlgAggErr " + e.getMessage());
        // return null;
        // }
        // })
        // .filter((key, value) -> value != null);

        // KTable<String, EnergyParamAgg> hourlyAggregated = hourlyAggStream
        // .groupByKey(Grouped.with("hourlyAggregated", Serdes.String(), new
        // EnergyParamSerde()))
        // .aggregate(
        // () -> new EnergyParamAgg(null, null).getDefaultValue(),
        // (aggKey, newValue, aggValue) -> {
        // try {
        // aggValue = new EnergyParamAgg(aggValue, newValue).getData();
        // } catch (Exception e) {
        // System.err.println("DlgAggError " + e.getMessage());
        // }
        // return aggValue;
        // },
        // Materialized
        // .<String, EnergyParamAgg, KeyValueStore<Bytes,
        // byte[]>>as("energy-param-hourly-agg-9")
        // .withKeySerde(Serdes.String())
        // .withValueSerde(new EnergyParamAggSerde()));

        // hourlyAggregated
        // .toStream()
        // .map((windowedKey, value) -> {
        // String keyname = windowedKey;
        // StreamKeyUtil key = new StreamKeyUtil("", "", 0, 0,
        // TimeBoundary.UNKNOWN).parseStreamKey(keyname);
        // value.setTimestamp(key.boundaries.startTime, key.boundaries.endTime);

        // System.out.println(
        // "hourly - " + "\nKey: " + key + "\nStart: " + key.boundaries.startTime + "
        // End: "
        // + key.boundaries.endTime + "\nValue:");
        // System.out.print(value.toJsonNode());
        // System.out.println("");

        // return KeyValue.pair(windowedKey, value);
        // })
        // .to("energy-parameter-hourly", Produced.with(Serdes.String(), new
        // EnergyParamAggSerde()));

        // ----- Hourly agg log end
        // ----------
        // **********

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler((t, e) -> {
            System.err.println(e);
            System.err.println("Stream error 1: " + e.getMessage());
        });
        streams.start();
        System.out.println("DlgIngestAggregatorApp started. Waiting for messages...");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

// {"comb_id":"3z3gc8h3-6",
// "meter_id":"6","dlg_id":"3z3gc8h3","zone_id":"q68sdl0a","org":"greencell_nuego","app_id":"null",
// "from":1752690600000,"to":1752777000000,"isAgg":true,
// "aggregatedLogs":{
// "energy_active_import":1146368.0, 1146368
// "energy_active_import_min":1.582920704E9, 1582918528
// "energy_active_import_max":1.584276992E9, 1584064896
// "energy_active_import_count":979,
// "energy_active_export":2.4399410000005446, 2.4399410000005446
// "energy_active_export_min":4963.928223, 4963.928223
// "energy_active_export_max":4966.671875, 4966.368164
// "energy_active_export_count":979
// }}