package com.example.topology.SmartMeterLopology.EnergyParameters.Aggregators;

import java.util.ArrayList;
import java.util.List;

import com.example.models.EnergyParameterModels.EnergyParamBaseModel;
import com.example.models.EnergyParameterModels.EnergyParameterPacketModel;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EnergyParamPacketsAggTopology {
    public List<EnergyParameterPacketModel> packets = new ArrayList<>();

    private static final ObjectMapper mapper = new ObjectMapper();

    public EnergyParamPacketsAggTopology(EnergyParamAgg acc, EnergyParamBaseModel curr) {
        if (acc != null && acc.packetsAgg != null && acc.packetsAgg.packets != null) {
            this.packets.addAll(acc.packetsAgg.packets);
        }

        if (curr != null) {
            EnergyParameterPacketModel data = new EnergyParameterPacketModel(curr).getData();
            if (data != null)
                this.packets.add(data);
        }
    }

    @JsonIgnore
    public EnergyParamPacketsAggTopology getData() {
        return this;
    }

    @JsonIgnore
    public JsonNode toJsonNode() {
        JsonNode result = mapper.valueToTree(this);
        return result;
    }

    public EnergyParamPacketsAggTopology parse(JsonNode packet) {
        if (packet.has("packets") && packet.get("packets").isArray()) {
            packet.get("packets").forEach(d -> {
                this.packets.add(new EnergyParameterPacketModel(null).parse(d));
            });
        }

        return this;
    }

}