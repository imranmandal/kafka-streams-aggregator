package com.example.topology.SmartMeterLopology.EnergyParameters.Aggregators;

import com.example.models.EnergyParameterModels.EnergyParamBaseModel;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EnergyParamAgg extends AggregationBaseTopology {

    public boolean isAgg = true;
    public EnergyParamAggLogsTopology aggregatedLogs = null;
    // public DailyAggTopology uptime;

    private static final ObjectMapper mapper = new ObjectMapper();

    @JsonIgnore
    private EnergyParamAgg defaultValue; // for serder to ignore defaultValue

    public EnergyParamAgg(EnergyParamAgg acc, EnergyParamBaseModel curr) {
        super(curr);

        if (acc == null || curr == null)
            return;

        try {
            this.aggregatedLogs = new EnergyParamAggLogsTopology(acc, curr).getData();
        } catch (Exception e) {
            System.err.println("EnergyParamAgg error" + e.getMessage());
        }

    }

    public EnergyParamAgg getDefaultValue() {
        return this;
    }

    public EnergyParamAgg parse(JsonNode packet) {
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

        if (packet.has("aggregatedLogs"))
            this.aggregatedLogs = new EnergyParamAggLogsTopology(null, null).parse(packet.get("aggregatedLogs"));

        return this;
    }

    // public EnergyParamAgg aggregate(EnergyParamAgg acc, EnergyParamBaseModel
    // curr) {
    // // this.dailyUptime = new DailyAggTopology().parse(acc, curr);
    // // need to handle rest agg here
    // this.aggregatedLogs = new EnergyParamAggLogsTopology(acc, curr).getData();

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
    public EnergyParamAgg getData() {
        return this;
    }
}
