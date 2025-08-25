package com.example.topology.SmartMeterTopology.InstantaneousParameters;

import com.example.DlgIngestAggregatorApp.AggregatorTopology;
import com.example.models.InstantaneousParameterModels.InstantaneousParamBaseModel;
import com.example.topology.AggregationBaseTopology;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class InstantaneousParamAggTopology extends AggregationBaseTopology {

    public InstantaneousParamAggMinMaxTopology durationLogsAgg = null;
    public InstantaneousParamPacketsAggTopology packetsAgg = null;
    AggregatorTopology[] topologyStages;
    // public DailyAggTopology uptime;

    private static final ObjectMapper mapper = new ObjectMapper();

    @JsonIgnore
    private InstantaneousParamAggTopology defaultValue; // for serder to ignore defaultValue

    public InstantaneousParamAggTopology(InstantaneousParamAggTopology acc, InstantaneousParamBaseModel curr,
            AggregatorTopology[] topologyStages) {
        super(curr);
        this.topologyStages = topologyStages;

        if (acc == null || curr == null)
            return;

        for (AggregatorTopology topology : topologyStages) {

            try {
                if (topology == AggregatorTopology.DURATION_LOG_AGG) {
                    this.durationLogsAgg = new InstantaneousParamAggMinMaxTopology(acc, curr).getData();
                } else if (topology == AggregatorTopology.PACKETS_AGG) {
                    this.packetsAgg = new InstantaneousParamPacketsAggTopology(acc, curr).getData();
                }
            } catch (Exception e) {
                System.err.println(topology + e.getMessage());
            }
        }

    }

    public InstantaneousParamAggTopology getDefaultValue() {
        return this;
    }

    public InstantaneousParamAggTopology parse(JsonNode packet) {
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

        if (packet.has("from") && packet.get("from").isNull() != true) {
            this.from = (packet.get("from").longValue());
        }

        if (packet.has("to") && packet.get("to").isNull() != true) {
            this.to = (packet.get("to").longValue());
        }

        if (packet.has("durationLogsAgg"))
            this.durationLogsAgg = new InstantaneousParamAggMinMaxTopology(null,
                    null).parse(packet.get("durationLogsAgg"));

        if (packet.has("packetsAgg"))
            this.packetsAgg = new InstantaneousParamPacketsAggTopology(null,
                    null).parse(packet.get("packetsAgg"));

        return this;

    }

    // public EnergyParamAgg aggregate(EnergyParamAgg acc, EnergyParamBaseModel
    // curr) {
    // // this.dailyUptime = new DailyAggTopology().parse(acc, curr);
    // // need to handle rest agg here
    // this.durationLogsAgg = new EnergyParamAggLogsTopology(acc, curr).getData();

    // return this;
    // }

    public void setTimestamp(long from, long to) {
        this.from = from;
        this.to = to;
    }

    @JsonIgnore
    public JsonNode toJsonNode() {
        JsonNode result = mapper.valueToTree(this);
        return result;
    }

    @JsonIgnore
    public InstantaneousParamAggTopology getData() {
        return this;
    }
}
