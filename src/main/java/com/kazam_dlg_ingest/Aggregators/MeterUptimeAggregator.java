package com.kazam_dlg_ingest.Aggregators;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import com.kazam_dlg_ingest.DlgIngestAggregatorApp.AggregatorTopology;
import com.kazam_dlg_ingest.models.MeterUptimeModels.MeterUptimeBaseModel;
import com.kazam_dlg_ingest.serder.MeterUptimeAggSerde;
import com.kazam_dlg_ingest.serder.MeterUptimeSerde;
import com.kazam_dlg_ingest.topology.SmartMeterTopology.MeterUptime.MeterUptimeAggTopology;
import com.kazam_dlg_ingest.utils.StreamKeyUtil;
import com.kazam_dlg_ingest.utils.TimeBoundaryUtil.TimeBoundary;

public class MeterUptimeAggregator {
    private KStream<String, MeterUptimeBaseModel> stream;
    private TimeBoundary boundaryUnit;
    private String groupName = "";
    private String storeName = "";
    private String outputTopic = "";
    private String keySuffix = "";
    AggregatorTopology[] topologyStages;

    public MeterUptimeAggregator(KStream<String, MeterUptimeBaseModel> stream,
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
        KStream<String, MeterUptimeBaseModel> aggStream = stream
                .map((key, data) -> {
                    try {
                        int utcOffsetSeconds = (data.utc_offset != null ? data.utc_offset : 0) * 60;
                        String streamKey = new StreamKeyUtil(data.comb_id, data.org,
                                data.timestamp,
                                utcOffsetSeconds, this.boundaryUnit)
                                .getStreamKey(this.keySuffix);

                        return KeyValue.pair(streamKey, data);
                    } catch (Exception e) {
                        System.err.println("MeterUptimeAggregatorErr " + e.getMessage());
                        return null;
                    }
                })
                .filter((key, value) -> value != null);

        KTable<String, MeterUptimeAggTopology> aggregated = aggStream
                .groupByKey(
                        Grouped.with(groupName, Serdes.String(), new MeterUptimeSerde()))
                .aggregate(
                        () -> new MeterUptimeAggTopology(null, null, this.topologyStages).getData(),
                        (aggKey, newValue, aggValue) -> {
                            try {
                                aggValue = new MeterUptimeAggTopology(aggValue, newValue, this.topologyStages)
                                        .getData();
                            } catch (Exception e) {
                                System.err.println("DlgAggError " + e.getMessage());
                            }
                            return aggValue;
                        },
                        Materialized
                                .<String, MeterUptimeAggTopology, KeyValueStore<Bytes, byte[]>>as(this.storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new MeterUptimeAggSerde(this.topologyStages)));

        aggregated
                .toStream()
                .map((windowedKey, value) -> {
                    String keyname = windowedKey;
                    StreamKeyUtil key = new StreamKeyUtil("", "", 0, 0, TimeBoundary.UNKNOWN).parseStreamKey(keyname);
                    value.setTimestamp(key.boundaries.startTime, key.boundaries.endTime);

                    System.out.println(
                            "\nMeterUptime\n" + boundaryUnit + "\nKey: " + key + "\nStart: "
                                    + key.boundaries.startTime + " End: "
                                    + key.boundaries.endTime + "\nValue:");
                    System.out.print(value.toJsonNode());
                    System.out.println("");

                    return KeyValue.pair(windowedKey, value);
                })
                .to(this.outputTopic,
                        Produced.with(Serdes.String(), new MeterUptimeAggSerde(this.topologyStages)));
    }
}
