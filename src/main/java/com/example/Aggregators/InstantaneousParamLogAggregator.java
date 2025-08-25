package com.example.Aggregators;

import com.example.DlgIngestAggregatorApp.AggregatorTopology;
import com.example.models.InstantaneousParameterModels.InstantaneousParamBaseModel;
import com.example.serder.InstantaneousParamAggSerde;
import com.example.serder.InstantaneousParamSerde;
import com.example.topology.SmartMeterTopology.InstantaneousParameters.InstantaneousParamAggTopology;
import com.example.utils.StreamKeyUtil;
import com.example.utils.TimeBoundaryUtil.TimeBoundary;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

public class InstantaneousParamLogAggregator {
    private KStream<String, InstantaneousParamBaseModel> stream;
    private TimeBoundary boundaryUnit;
    private String groupName = "";
    private String storeName = "";
    private String outputTopic = "";
    private String keySuffix = "";
    AggregatorTopology[] topologyStages;

    public InstantaneousParamLogAggregator(KStream<String, InstantaneousParamBaseModel> stream,
            TimeBoundary boundaryUnit, String storeName,
            String outputTopic, AggregatorTopology[] topologyStages, String keySuffix) {
        this.stream = stream;
        this.boundaryUnit = boundaryUnit;
        this.storeName = storeName;
        this.outputTopic = outputTopic;
        this.groupName = outputTopic;
        this.topologyStages = topologyStages;
        this.keySuffix = keySuffix;
    }

    public void aggregate() {
        KStream<String, InstantaneousParamBaseModel> aggStream = stream
                .map((key, data) -> {
                    try {
                        int utcOffsetSeconds = (data.utc_offset != null ? data.utc_offset : 0) * 60;
                        String streamKey = new StreamKeyUtil(data.comb_id, data.org,
                                data.timestamp,
                                utcOffsetSeconds, this.boundaryUnit)
                                .getStreamKey(this.keySuffix);

                        return KeyValue.pair(streamKey, data);
                    } catch (Exception e) {
                        System.err.println("DlgAggErr " + e.getMessage());
                        return null;
                    }
                })
                .filter((key, value) -> value != null);

        KTable<String, InstantaneousParamAggTopology> aggregated = aggStream
                .groupByKey(
                        Grouped.with(groupName, Serdes.String(), new InstantaneousParamSerde()))
                .aggregate(
                        () -> new InstantaneousParamAggTopology(null, null, this.topologyStages).getDefaultValue(),
                        (aggKey, newValue, aggValue) -> {
                            try {
                                aggValue = new InstantaneousParamAggTopology(aggValue, newValue, this.topologyStages)
                                        .getData();
                            } catch (Exception e) {
                                System.err.println("DlgAggError " + e.getMessage());
                            }
                            return aggValue;
                        },
                        Materialized
                                .<String, InstantaneousParamAggTopology, KeyValueStore<Bytes, byte[]>>as(this.storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new InstantaneousParamAggSerde(this.topologyStages)));

        aggregated
                .toStream()
                .map((windowedKey, value) -> {
                    String keyname = windowedKey;
                    StreamKeyUtil key = new StreamKeyUtil("", "", 0, 0, TimeBoundary.UNKNOWN).parseStreamKey(keyname);
                    value.setTimestamp(key.boundaries.startTime, key.boundaries.endTime);

                    System.out.println(
                            "\nInstantaneous\n" + boundaryUnit + "\nKey: " + key + "\nStart: " + key.boundaries.startTime + " End: "
                                    + key.boundaries.endTime + "\nValue:");
                    System.out.print(value.toJsonNode());
                    System.out.println("");

                    return KeyValue.pair(windowedKey, value);
                })
                .to(this.outputTopic,
                        Produced.with(Serdes.String(), new InstantaneousParamAggSerde(this.topologyStages)));
    }
}