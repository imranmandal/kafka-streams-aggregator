package com.kazam_dlg_ingest.models.InstantaneousParameterModels;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class InstantaneousParameterPacketModel {
    public double voltage_r = 0;
    public double voltage_y = 0;
    public double voltage_b = 0;
    public double voltage_avg = 0;
    public double curr_r = 0;
    public double curr_y = 0;
    public double curr_b = 0;
    public double curr_n = 0;
    public double pf_r = 0;
    public double pf_y = 0;
    public double pf_b = 0;
    public double pf_avg = 0;
    public double power_active_r = 0;
    public double power_active_y = 0;
    public double power_active_b = 0;
    public double power_active_sum = 0;
    public double power_reactive_r = 0;
    public double power_reactive_y = 0;
    public double power_reactive_b = 0;
    public double power_reactive_sum = 0;
    public double power_apparent_r = 0;
    public double power_apparent_y = 0;
    public double power_apparent_b = 0;
    public double power_apparent_sum = 0;
    public double freq = 0;
    public double voltage_thd_r = 0;
    public double voltage_thd_y = 0;
    public double voltage_thd_b = 0;
    public double curr_thd_r = 0;
    public double curr_thd_y = 0;
    public double curr_thd_b = 0;
    public Long timestamp;
    public Boolean error;

    private static final ObjectMapper mapper = new ObjectMapper();

    public InstantaneousParameterPacketModel(InstantaneousParamBaseModel data) {
        if (data == null)
            return;

        this.voltage_r = data.instantaneousParams.get("voltage_r").doubleValue();
        this.voltage_y = data.instantaneousParams.get("voltage_y").doubleValue();
        this.voltage_b = data.instantaneousParams.get("voltage_b").doubleValue();
        this.voltage_avg = data.instantaneousParams.get("voltage_avg").doubleValue();
        this.curr_r = data.instantaneousParams.get("curr_r").doubleValue();
        this.curr_y = data.instantaneousParams.get("curr_y").doubleValue();
        this.curr_b = data.instantaneousParams.get("curr_b").doubleValue();
        this.curr_n = data.instantaneousParams.get("curr_n").doubleValue();
        this.pf_r = data.instantaneousParams.get("pf_r").doubleValue();
        this.pf_y = data.instantaneousParams.get("pf_y").doubleValue();
        this.pf_b = data.instantaneousParams.get("pf_b").doubleValue();
        this.pf_avg = data.instantaneousParams.get("pf_avg").doubleValue();
        this.power_active_r = data.instantaneousParams.get("power_active_r").doubleValue();
        this.power_active_y = data.instantaneousParams.get("power_active_y").doubleValue();
        this.power_active_b = data.instantaneousParams.get("power_active_b").doubleValue();
        this.power_active_sum = data.instantaneousParams.get("power_active_sum").doubleValue();
        this.power_reactive_r = data.instantaneousParams.get("power_reactive_r").doubleValue();
        this.power_reactive_y = data.instantaneousParams.get("power_reactive_y").doubleValue();
        this.power_reactive_b = data.instantaneousParams.get("power_reactive_b").doubleValue();
        this.power_reactive_sum = data.instantaneousParams.get("power_reactive_sum").doubleValue();
        this.power_apparent_r = data.instantaneousParams.get("power_apparent_r").doubleValue();
        this.power_apparent_y = data.instantaneousParams.get("power_apparent_y").doubleValue();
        this.power_apparent_b = data.instantaneousParams.get("power_apparent_b").doubleValue();
        this.power_apparent_sum = data.instantaneousParams.get("power_apparent_sum").doubleValue();
        this.freq = data.instantaneousParams.get("freq").doubleValue();
        this.voltage_thd_r = data.instantaneousParams.get("voltage_thd_r").doubleValue();
        this.voltage_thd_y = data.instantaneousParams.get("voltage_thd_y").doubleValue();
        this.voltage_thd_b = data.instantaneousParams.get("voltage_thd_b").doubleValue();
        this.curr_thd_r = data.instantaneousParams.get("curr_thd_r").doubleValue();
        this.curr_thd_y = data.instantaneousParams.get("curr_thd_y").doubleValue();
        this.curr_thd_b = data.instantaneousParams.get("curr_thd_b").doubleValue();

        this.timestamp = data.timestamp * 1000;
        this.error = data.error;
    }

    @JsonIgnore
    public InstantaneousParameterPacketModel getData() {
        return this;
    }

    @JsonIgnore
    public JsonNode toJsonNode() {
        JsonNode result = mapper.valueToTree(this);
        return result;
    }

    public InstantaneousParameterPacketModel parse(JsonNode packet) {
        if (packet == null)
            return this;

        this.voltage_r = packet.get("voltage_r").asDouble();
        this.voltage_y = packet.get("voltage_y").asDouble();
        this.voltage_b = packet.get("voltage_b").asDouble();
        this.voltage_avg = packet.get("voltage_avg").asDouble();
        this.curr_r = packet.get("curr_r").asDouble();
        this.curr_y = packet.get("curr_y").asDouble();
        this.curr_b = packet.get("curr_b").asDouble();
        this.curr_n = packet.get("curr_n").asDouble();
        this.pf_r = packet.get("pf_r").asDouble();
        this.pf_y = packet.get("pf_y").asDouble();
        this.pf_b = packet.get("pf_b").asDouble();
        this.pf_avg = packet.get("pf_avg").asDouble();
        this.power_active_r = packet.get("power_active_r").asDouble();
        this.power_active_y = packet.get("power_active_y").asDouble();
        this.power_active_b = packet.get("power_active_b").asDouble();
        this.power_active_sum = packet.get("power_active_sum").asDouble();
        this.power_reactive_r = packet.get("power_reactive_r").asDouble();
        this.power_reactive_y = packet.get("power_reactive_y").asDouble();
        this.power_reactive_b = packet.get("power_reactive_b").asDouble();
        this.power_reactive_sum = packet.get("power_reactive_sum").asDouble();
        this.power_apparent_r = packet.get("power_apparent_r").asDouble();
        this.power_apparent_y = packet.get("power_apparent_y").asDouble();
        this.power_apparent_b = packet.get("power_apparent_b").asDouble();
        this.power_apparent_sum = packet.get("power_apparent_sum").asDouble();
        this.freq = packet.get("freq").asDouble();
        this.voltage_thd_r = packet.get("voltage_thd_r").asDouble();
        this.voltage_thd_y = packet.get("voltage_thd_y").asDouble();
        this.voltage_thd_b = packet.get("voltage_thd_b").asDouble();
        this.curr_thd_r = packet.get("curr_thd_r").asDouble();
        this.curr_thd_y = packet.get("curr_thd_y").asDouble();
        this.curr_thd_b = packet.get("curr_thd_b").asDouble();

        this.timestamp = packet.get("timestamp").asLong();
        this.error = packet.get("error").asBoolean();
        return this;
    }

}
