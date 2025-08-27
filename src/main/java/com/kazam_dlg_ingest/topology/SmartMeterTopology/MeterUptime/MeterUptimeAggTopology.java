package com.kazam_dlg_ingest.topology.SmartMeterTopology.MeterUptime;

import com.kazam_dlg_ingest.DlgIngestAggregatorApp.AggregatorTopology;
import com.kazam_dlg_ingest.models.MeterUptimeModels.MeterUptimeBaseModel;
import com.kazam_dlg_ingest.topology.AggregationBaseTopology;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MeterUptimeAggTopology extends AggregationBaseTopology {
    private static final ObjectMapper mapper = new ObjectMapper();
    AggregatorTopology[] topologyStages;

    public Integer packetCount = 0;
    public MeterUptimePacketsAggTopology packetsAgg = new MeterUptimePacketsAggTopology(null, null).getData();

    public MeterUptimeAggTopology(MeterUptimeAggTopology agg, MeterUptimeBaseModel packet,
            AggregatorTopology[] topologyStages) {

        super(packet);

        this.topologyStages = topologyStages;

        if (agg == null || packet == null)
            return;

        try {
            for (AggregatorTopology topology : this.topologyStages) {
                if (topology == AggregatorTopology.DURATION_LOG_AGG) {
                    if (agg != null)
                        this.packetCount = agg.packetCount + 1;
                } else if (topology == AggregatorTopology.PACKETS_AGG) {
                    this.packetsAgg = new MeterUptimePacketsAggTopology(agg, packet).getData();
                }

            }
        } catch (Exception e) {
            System.err.println("MeterUptimeAggTopology" + e.getMessage());
        }
    }

    @JsonIgnore
    public MeterUptimeAggTopology getData() {
        return this;
    }

    public void setTimestamp(long from, long to) {
        this.from = from;
        this.to = to;
        this.packetsAgg.total_time = to - from;
    }

    public MeterUptimeAggTopology parse(JsonNode packet) {
        if (packet == null)
            return this;

        if (packet.has("comb_id"))
            this.comb_id = packet.get("comb_id").asText();

        if (packet.has("meter_id"))
            this.meter_id = packet.get("meter_id").asText();

        if (packet.has("dlg_id"))
            this.dlg_id = packet.get("dlg_id").asText();

        if (packet.has("zone_id"))
            this.zone_id = packet.get("zone_id").asText();

        if (packet.has("org"))
            this.org = packet.get("org").asText();

        if (packet.has("app_id"))
            this.app_id = packet.get("app_id").asText();

        if (packet.has("from") && packet.get("from") != null && !packet.get("from").isNull()) {
            this.from = (packet.get("from").longValue());
        }

        if (packet.has("to") && packet.get("to") != null && !packet.get("to").isNull()) {
            this.to = (packet.get("to").longValue());
        }

        if (packet.has("utc_offset") && packet.get("utc_offset") != null && !packet.get("utc_offset").isNull()) {
            this.utc_offset = (packet.get("utc_offset").intValue());
        }

        if (packet.has("packetCount"))
            this.packetCount = packet.get("packetCount").intValue();

        if (packet.has("packetsAgg"))
            this.packetsAgg = new MeterUptimePacketsAggTopology(
                    null,
                    null)
                    .parse(packet.get("packetsAgg"));

        return this;

    }

    public JsonNode toJsonNode() {
        JsonNode result = mapper.valueToTree(this);
        return result;
    }
}