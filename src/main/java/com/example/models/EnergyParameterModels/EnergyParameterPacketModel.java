package com.example.models.EnergyParameterModels;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EnergyParameterPacketModel {
    public double energy_active_export = 0;
    public double energy_active_import = 0;
    public Long timestamp;
    public Boolean error;

    private static final ObjectMapper mapper = new ObjectMapper();

    public EnergyParameterPacketModel(EnergyParamBaseModel data) {
        if (data == null)
            return;

        this.energy_active_export = data.energy_active_export;
        this.energy_active_import = data.energy_active_import;
        this.timestamp = data.timestamp * 1000;
        this.error = data.error;
    }

    @JsonIgnore
    public EnergyParameterPacketModel getData() {
        return this;
    }

    @JsonIgnore
    public JsonNode toJsonNode() {
        JsonNode result = mapper.valueToTree(this);
        return result;
    }

    public EnergyParameterPacketModel parse(JsonNode packet) {
        if (packet == null)
            return this;

        this.energy_active_export = packet.get("energy_active_export").asDouble();
        this.energy_active_import = packet.get("energy_active_import").asDouble();
        this.timestamp = packet.get("timestamp").asLong();
        this.error = packet.get("error").asBoolean();
        return this;
    }

}
