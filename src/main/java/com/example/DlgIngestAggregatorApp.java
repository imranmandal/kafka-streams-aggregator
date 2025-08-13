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

    public enum AggregatorTopology {
        DURATION_LOG_AGG,
        PACKETS_AGG,
    }

    // function to generate a random string of length n
    static String getSuffix(int n) {

        // choose a Character random from this String
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {

            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index = (int) (AlphaNumericString.length()
                    * Math.random());

            // add Character one by one in end of sb
            sb.append(AlphaNumericString
                    .charAt(index));
        }

        return sb.toString();
    }

    public static void main(String args[]) {

        String testSuffix = "-test-11";
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregator-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeExtractor.class);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, EnergyParamBaseModel> stream = builder.stream("energy-parameter" + testSuffix,
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

        // // ----- Daily agg log
        // AggregatorTopology[] dailyAggTopologyStages = {
        // AggregatorTopology.DURATION_LOG_AGG,
        // AggregatorTopology.PACKETS_AGG
        // };
        // new LogAggregator(stream, TimeBoundary.DAY,
        // "energy-param-daily-agg-store" + testSuffix,
        // "energy-param-daily-agg" + testSuffix,
        // dailyAggTopologyStages,
        // testSuffix + getSuffix(4))
        // .aggregate();

        // ----- Hourly agg log
        AggregatorTopology[] hourlyAggTopologyStages = {
                AggregatorTopology.DURATION_LOG_AGG,
                AggregatorTopology.PACKETS_AGG
        };
        new LogAggregator(
                stream,
                TimeBoundary.HOUR,
                "energy-param-hourly-agg-store" + testSuffix,
                "energy-param-hourly-agg" + testSuffix,
                hourlyAggTopologyStages,
                testSuffix + getSuffix(4))
                .aggregate();

        // KGroupedStream<String, EnergyParamBaseModel> groupStream = stream
        // .map((key, energyParam) -> {
        // try {
        // int utcOffsetSeconds = (energyParam.utc_offset != null ?
        // energyParam.utc_offset : 0) * 60;
        // String streamKey = new StreamKeyUtil(energyParam.comb_id, energyParam.org,
        // energyParam.timestamp,
        // utcOffsetSeconds, TimeBoundary.HOUR)
        // .getStreamKey();

        // return KeyValue.pair(streamKey, energyParam);
        // } catch (Exception e) {
        // System.err.println("DlgAggErr " + e.getMessage());
        // return null;
        // }
        // })
        // .filter((key, value) -> value != null)
        // .groupByKey(Grouped.with("groupName", Serdes.String(), new
        // EnergyParamSerde()))

        // ;

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler((t, e) -> {
            System.err.println(e);
            System.err.println("Stream error 1: " + e.getMessage());
        });
        streams.cleanUp();
        streams.start();
        System.out.println("DlgIngestAggregatorApp started. Waiting for messages...");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

// {"comb_id":"3z3gc8h3-6",
// "meter_id":"6","dlg_id":"3z3gc8h3","zone_id":"q68sdl0a","org":"greencell_nuego","app_id":"null",
// "from":1752690600000,"to":1752777000000,"isAgg":true,
// "durationLogsAgg":{
// "energy_active_import":1146368.0, 1146368
// "energy_active_import_min":1.582920704E9, 1582918528
// "energy_active_import_max":1.584276992E9, 1584064896
// "energy_active_import_count":979,
// "energy_active_export":2.4399410000005446, 2.4399410000005446
// "energy_active_export_min":4963.928223, 4963.928223
// "energy_active_export_max":4966.671875, 4966.368164
// "energy_active_export_count":979
// }}