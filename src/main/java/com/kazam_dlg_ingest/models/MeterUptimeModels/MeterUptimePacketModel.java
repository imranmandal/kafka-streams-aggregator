package com.kazam_dlg_ingest.models.MeterUptimeModels;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MeterUptimePacketModel {
    private static final ObjectMapper mapper = new ObjectMapper();

    public Boolean network = true;
    public Long timestamp = null;

    public MeterUptimePacketModel(MeterUptimeBaseModel packet) {
        if (packet == null)
            return;

        try {
            if (packet.timestamp != null) {
                this.timestamp = packet.timestamp;
            }

        } catch (Exception e) {
            System.err.println("UptimePacketBaseModel error " + e.getMessage());
        }

    }

    @JsonIgnore
    public MeterUptimePacketModel getData() {
        return this;
    }

    public JsonNode toJsonNode() {
        return mapper.valueToTree(this);
    }

    public MeterUptimePacketModel parse(JsonNode packet) {
        if (packet.has("timestamp")) {
            this.timestamp = packet.get("timestamp").asLong();
        }
        
        return this;
    }
}
