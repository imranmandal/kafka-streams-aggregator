package com.kazam_dlg_ingest.topology.SmartMeterTopology.MeterUptime;

import com.kazam_dlg_ingest.models.MeterUptimeModels.MeterUptimeBaseModel;
import com.kazam_dlg_ingest.models.MeterUptimeModels.MeterUptimePacketModel;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

public class MeterUptimePacketsAggTopology {
    public Integer packetCount = 0;
    public List<MeterUptimePacketModel> packets = new ArrayList<>();

    public MeterUptimePacketsAggTopology(MeterUptimeAggTopology agg, MeterUptimeBaseModel packet) {

        if (agg == null || packet == null)
            return;

        if (agg != null)
            if (agg.packetsAgg != null) {
                if (agg.packetsAgg.packets.size() > 0) {
                    this.packets.addAll(agg.packetsAgg.packets);
                }

                if (packet != null) {
                    this.packetCount = agg.packetsAgg.packetCount + 1;
                }
            }

        if (packet != null) {
            this.packets.add(new MeterUptimePacketModel(packet).getData());
            this.packetCount = 1;

            // if (this.online_time == 0L)
            // this.online_time = packet.online_time;
        }
    }

    @JsonIgnore
    public MeterUptimePacketsAggTopology getData() {
        return this;
    }

    public MeterUptimePacketsAggTopology parse(JsonNode aggData) {
        JsonNode totalCountJsonNode = aggData.get("packetCount");
        if (totalCountJsonNode != null && !totalCountJsonNode.isNull()) {
            this.packetCount = totalCountJsonNode.intValue();
        }

        if (aggData.has("packets") && aggData.get("packets").isArray()) {
            aggData.get("packets").forEach(d -> {
                this.packets.add(new MeterUptimePacketModel(null).parse(d));
            });
        }

        return this;
    }
}
