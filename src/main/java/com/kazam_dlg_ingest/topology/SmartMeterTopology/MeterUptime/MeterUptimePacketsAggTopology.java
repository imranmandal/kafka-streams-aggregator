package com.kazam_dlg_ingest.topology.SmartMeterTopology.MeterUptime;

import com.kazam_dlg_ingest.models.MeterUptimeModels.MeterUptimeBaseModel;
import com.kazam_dlg_ingest.models.MeterUptimeModels.MeterUptimePacketModel;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

public class MeterUptimePacketsAggTopology {
    public Long total_time = 0L;
    public Long online_time = 0L;
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
                    this.online_time = agg.packetsAgg.online_time + packet.online_time;
                }
            }

        if (packet != null) {
            this.packets.add(new MeterUptimePacketModel(packet).getData());

            if (this.online_time == 0L)
                this.online_time = packet.online_time;
        }
    }

    @JsonIgnore
    public MeterUptimePacketsAggTopology getData() {
        return this;
    }

    public MeterUptimePacketsAggTopology parse(JsonNode aggData) {
        JsonNode totalTimeJsonNode = aggData.get("total_time");
        if (totalTimeJsonNode != null && !totalTimeJsonNode.isNull()) {
            this.total_time = totalTimeJsonNode.longValue();
        }

        JsonNode onlineTimeJsonNode = aggData.get("online_time");
        if (onlineTimeJsonNode != null && !onlineTimeJsonNode.isNull()) {
            this.online_time = onlineTimeJsonNode.longValue();
        }

        if (aggData.has("packets") && aggData.get("packets").isArray()) {
            aggData.get("packets").forEach(d -> {
                this.packets.add(new MeterUptimePacketModel(null).parse(d));
            });
        }

        return this;
    }
}
