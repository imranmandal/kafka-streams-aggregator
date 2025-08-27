package com.kazam_dlg_ingest;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.common.serialization.Serdes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kazam_dlg_ingest.DlgIngestAggregatorApp.AggregatorTopology;
import com.kazam_dlg_ingest.Aggregators.EnergyParamLogAggregator;
import com.kazam_dlg_ingest.Aggregators.InstantaneousParamLogAggregator;
import com.kazam_dlg_ingest.Aggregators.MeterUptimeAggregator;
import com.kazam_dlg_ingest.models.EnergyParameterModels.EnergyParamBaseModel;
import com.kazam_dlg_ingest.models.InstantaneousParameterModels.InstantaneousParamBaseModel;
import com.kazam_dlg_ingest.models.MeterUptimeModels.MeterUptimeBaseModel;
import com.kazam_dlg_ingest.utils.EventTimeExtractor;
import com.kazam_dlg_ingest.utils.TimeBoundaryUtil.TimeBoundary;

public class DlgIngestAggregatorTestApp {
    private static final ObjectMapper mapper = new ObjectMapper();

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
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

        String keySuffix = dotenv.get("TEST_SUFFIX", "-test-11");
        String app_id = dotenv.get("APPLICATION_ID", "aggregator-app");
        String kafka_broker = dotenv.get("KAFKA_BROKER", "localhost:9092");

        System.out.println("kafka_broker " + kafka_broker);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, app_id);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_broker);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeExtractor.class);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MeterUptimeBaseModel> meterUptimeStream = builder.stream("meter-uptime" + keySuffix,
                Consumed.with(Serdes.String(), Serdes.String()))
                .map((key, value) -> {
                    try {
                        JsonNode json = mapper.readTree(value);
                        MeterUptimeBaseModel data = new MeterUptimeBaseModel(json).getData();
                        return KeyValue.pair(key, data);
                    } catch (Exception e) {
                        System.err.println("DlgAggErr " + e.getMessage());
                        return null;
                    }
                })
                .filter((key, value) -> value != null);

        KStream<String, EnergyParamBaseModel> energyParamStream = builder.stream("energy-parameter" + keySuffix,
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

        KStream<String, InstantaneousParamBaseModel> instantParamStream = builder
                .stream("instantaneous-parameter" + keySuffix,
                        Consumed.with(Serdes.String(), Serdes.String()))
                .map((key, value) -> {
                    try {
                        JsonNode json = mapper.readTree(value);
                        InstantaneousParamBaseModel data = new InstantaneousParamBaseModel(json).getData();

                        return KeyValue.pair(key, data);
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
        // "energy-param-daily-agg-store" + keySuffix,
        // "energy-param-daily-agg" + keySuffix,
        // dailyAggTopologyStages,
        // keySuffix + getSuffix(4))
        // .aggregate();

        // ----- daily agg log
        AggregatorTopology[] dailyAggTopologyStages = {
                AggregatorTopology.DURATION_LOG_AGG,
        };
        new EnergyParamLogAggregator(
                energyParamStream,
                TimeBoundary.HOUR,
                "energy-param-daily-agg-store" + keySuffix,
                "energy-param-daily-agg" + keySuffix,
                dailyAggTopologyStages,
                keySuffix + getSuffix(4))
                .aggregate();

        new InstantaneousParamLogAggregator(
                instantParamStream,
                TimeBoundary.HOUR,
                "instantaneous-param-daily-agg-store" + keySuffix,
                "instantaneous-param-daily-agg" + keySuffix,
                dailyAggTopologyStages,
                keySuffix + getSuffix(4))
                .aggregate();

        new MeterUptimeAggregator(
                meterUptimeStream,
                TimeBoundary.HOUR,
                "meter-uptime-daily-agg-store" + keySuffix,
                "meter-uptime-daily-agg" + keySuffix,
                dailyAggTopologyStages,
                keySuffix + getSuffix(4))
                .aggregate();

        // // ----- Hourly agg log
        // AggregatorTopology[] hourlyAggTopologyStages = {
        // AggregatorTopology.DURATION_LOG_AGG,
        // AggregatorTopology.PACKETS_AGG
        // };
        // new EnergyParamLogAggregator(
        // energyParamStream,
        // TimeBoundary.HOUR,
        // "energy-param-hourly-agg-store" + keySuffix,
        // "energy-param-hourly-agg" + keySuffix,
        // hourlyAggTopologyStages,
        // keySuffix + getSuffix(4))
        // .aggregate();

        // new InstantaneousParamLogAggregator(
        // instantParamStream,
        // TimeBoundary.HOUR,
        // "instantaneous-param-hourly-agg-store" + keySuffix,
        // "instantaneous-param-hourly-agg" + keySuffix,
        // hourlyAggTopologyStages,
        // keySuffix + getSuffix(4))
        // .aggregate();

        // new MeterUptimeAggregator(
        // meterUptimeStream,
        // TimeBoundary.HOUR,
        // "meter-uptime-hourly-agg-store" + keySuffix,
        // "meter-uptime-hourly-agg" + keySuffix,
        // hourlyAggTopologyStages,
        // keySuffix + getSuffix(4))
        // .aggregate();

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

        try {
            new HealthCheckServer(streams).start();
        } catch (IOException e1) {
            System.err.println("Http Server error: \n" + e1);
        }

        streams.cleanUp();
        streams.start();
        System.out.println("🪇 DlgIngestAggregatorTestApp started. Waiting for messages...");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
