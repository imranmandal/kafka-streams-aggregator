package com.kazam_dlg_ingest.topology.SmartMeterTopology.MeterUptime;

import com.fasterxml.jackson.databind.JsonNode;
import com.kazam_dlg_ingest.models.MeterUptimeModels.MeterUptimeBaseModel;

public class MeterUptimeLogTopology {

    public Long total_time = 0L;
    public Long online_time = 0L;

    public MeterUptimeLogTopology(MeterUptimeAggTopology agg, MeterUptimeBaseModel packet) {

        if (agg == null || packet == null)
            return;

        if (agg != null)
            if (agg.durationLogAgg != null) {
                if (packet != null) {
                    this.online_time = agg.durationLogAgg.online_time + packet.online_time;
                }
            }

        if (packet != null) {
            if (this.online_time == 0L)
                this.online_time = packet.online_time;
        }
    }

    public MeterUptimeLogTopology parse(JsonNode aggData) {

        JsonNode totalTimeJsonNode = aggData.get("total_time");
        if (totalTimeJsonNode != null && !totalTimeJsonNode.isNull()) {
            this.total_time = totalTimeJsonNode.longValue();
        }

        JsonNode onlineTimeJsonNode = aggData.get("online_time");
        if (onlineTimeJsonNode != null && !onlineTimeJsonNode.isNull()) {
            this.online_time = onlineTimeJsonNode.longValue();
        }

        return this;

    }
}
