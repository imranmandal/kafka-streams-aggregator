package com.example.Aggregators;

import com.example.models.EnergyParameterModels.EnergyParamBaseModel;
import com.example.serder.EnergyParamAggSerde;
import com.example.serder.EnergyParamSerde;
import com.example.topology.SmartMeterLopology.EnergyParameters.Aggregators.EnergyParamAgg;
import com.example.utils.StreamKeyUtil;
import com.example.utils.TimeBoundaryUtil;
import com.example.utils.TimeBoundaryUtil.TimeBoundary;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

public class LogAggregator {
    private KStream<String, EnergyParamBaseModel> stream;
    private TimeBoundary boundaryUnit;
    private String groupName = "";
    private String storeName = "";
    private String outputTopic = "";

    public LogAggregator(KStream<String, EnergyParamBaseModel> stream, TimeBoundary boundaryUnit, String storeName,
            String outputTopic) {
        this.stream = stream;
        this.boundaryUnit = boundaryUnit;
        this.storeName = storeName;
        this.outputTopic = outputTopic;
        this.groupName = outputTopic;
    }

    public void aggregate() {
        KStream<String, EnergyParamBaseModel> aggStream = stream
                // .map((key, energyParam) -> {
                // try {
                // int utcOffsetSeconds = (energyParam.utc_offset != null ?
                // energyParam.utc_offset : 0) * 60;
                // String streamKey = new StreamKeyUtil(energyParam.comb_id, energyParam.org,
                // energyParam.timestamp,
                // utcOffsetSeconds, this.boundaryUnit)
                // .getStreamKey();

                // return KeyValue.pair(streamKey, energyParam);
                // } catch (Exception e) {
                // System.err.println("DlgAggErr " + e.getMessage());
                // return null;
                // }
                // })

                .flatMap((key, energyParam) -> {
                    List<KeyValue<String, EnergyParamBaseModel>> results = new ArrayList<>();
                    try {
                        int utcOffsetSeconds = (energyParam.utc_offset != null ? energyParam.utc_offset : 0) * 60;
                        StreamKeyUtil streamKeyUtil = new StreamKeyUtil(energyParam.comb_id, energyParam.org,
                                energyParam.timestamp,
                                utcOffsetSeconds, this.boundaryUnit);

                        String streamKey = streamKeyUtil.getStreamKey();

                        /*
                         * 1. check prevPacket timestamp boundary
                         * 2. if the prevPacket timestamp is less than 10 sec then split the usage
                         * 3. Exact timestamp and prevPacket is not referred in next steps.
                         * 4. Timestamp needs to be within the duration (required).
                         * 
                         * ---- Usage-Split Logic -
                         * 1. divide the delta usage by diff
                         */

                        if (energyParam.prevEnergyParamPkt != null) {
                            // split the packet here
                            TimeBoundaryUtil boundaries = streamKeyUtil.boundaries;

                            /*
                             * checking if the prevPacket timestamp is greater than or equal to 10sec from
                             * currPacket duration boundary
                             * 
                             * This step will execute -
                             * if currPacket timestamp is 2025-06-29T18:30:00.000+00:00 and prevPacket
                             * timestamp is 2025-06-29T18:29:50.000+00:00
                             */

                            if (boundaries.startTime - energyParam.prevEnergyParamPkt.timestamp * 1000 > 10000) {
                                int diff = 2;
                                long timestamp = energyParam.prevEnergyParamPkt.timestamp;

                                while (true) {
                                    if (this.boundaryUnit == TimeBoundary.DAY) {
                                        timestamp += 60 * 60 * 24; // adding day seconds
                                    } else if (this.boundaryUnit == TimeBoundary.HOUR) {
                                        timestamp += 60 * 60; // adding hour seconds
                                    }

                                    if (boundaries.startTime <= timestamp * 1000)
                                        break;
                                    else
                                        diff++;
                                }

                                timestamp = energyParam.prevEnergyParamPkt.timestamp;
                                double energy_import_delta_split = energyParam.energy_active_import_delta / diff;
                                double energy_export_delta_split = energyParam.energy_active_export_delta / diff;

                                double energy_import_ = energyParam.prevEnergyParamPkt.energy_active_import;
                                double energy_export_ = energyParam.prevEnergyParamPkt.energy_active_export;

                                for (int i = 0; i < diff; i++) {
                                    EnergyParamBaseModel packet = new EnergyParamBaseModel(null);

                                    packet.comb_id = energyParam.comb_id;
                                    packet.meter_id = energyParam.meter_id;
                                    packet.dlg_id = energyParam.dlg_id;
                                    packet.zone_id = energyParam.zone_id;
                                    packet.org = energyParam.org;
                                    packet.app_id = energyParam.app_id;
                                    packet.timestamp = timestamp;
                                    packet.utc_offset = energyParam.utc_offset;
                                    packet.error = energyParam.error;

                                    packet.energy_active_import = energy_import_ + energy_import_delta_split;
                                    packet.energy_active_export = energy_export_ + energy_export_delta_split;
                                    packet.energy_active_import_delta = energy_import_delta_split;
                                    packet.energy_active_export_delta = energy_export_delta_split;

                                    energy_import_ = packet.energy_active_import;
                                    energy_export_ = packet.energy_active_export;

                                    String streamKeyStr = new StreamKeyUtil(energyParam.comb_id,
                                            energyParam.org,
                                            timestamp,
                                            utcOffsetSeconds, this.boundaryUnit).getStreamKey();

                                    results.add(KeyValue.pair(streamKeyStr, packet));

                                    // increment timestamp
                                    if (this.boundaryUnit == TimeBoundary.DAY) {
                                        timestamp += 60 * 60 * 24; // adding day seconds
                                    } else if (this.boundaryUnit == TimeBoundary.HOUR) {
                                        timestamp += 60 * 60; // adding hour seconds
                                    }

                                    if (timestamp > energyParam.timestamp) {
                                        timestamp = energyParam.timestamp;
                                    }
                                }
                                // }
                                return results;
                            }
                        }

                        // default append
                        results.add(KeyValue.pair(streamKey, energyParam));
                    } catch (Exception e) {
                        System.err.println("DlgAggErr " + e.getMessage());
                    }

                    // default return
                    return results;
                })
                .filter((key, value) -> value != null);

        KTable<String, EnergyParamAgg> aggregated = aggStream
                .groupByKey(Grouped.with(groupName, Serdes.String(), new EnergyParamSerde()))
                .aggregate(
                        () -> new EnergyParamAgg(null, null).getDefaultValue(),
                        (aggKey, newValue, aggValue) -> {
                            try {
                                aggValue = new EnergyParamAgg(aggValue, newValue).getData();
                            } catch (Exception e) {
                                System.err.println("DlgAggError " + e.getMessage());
                            }
                            return aggValue;
                        },
                        Materialized
                                .<String, EnergyParamAgg, KeyValueStore<Bytes, byte[]>>as(this.storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new EnergyParamAggSerde()));

        aggregated
                .toStream()
                .map((windowedKey, value) -> {
                    String keyname = windowedKey;
                    StreamKeyUtil key = new StreamKeyUtil("", "", 0, 0, TimeBoundary.UNKNOWN).parseStreamKey(keyname);
                    value.setTimestamp(key.boundaries.startTime, key.boundaries.endTime);

                    return KeyValue.pair(windowedKey, value);
                })
                .to(this.outputTopic, Produced.with(Serdes.String(), new EnergyParamAggSerde()));
    }
}