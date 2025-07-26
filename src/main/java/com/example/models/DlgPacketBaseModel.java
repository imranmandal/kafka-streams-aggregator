package com.example.models;

import com.example.models.EnergyParameterModels.EnergyParamBaseModel;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DlgPacketBaseModel {
    public String comb_id;
    public String meter_id;
    public String dlg_id;
    public String zone_id;
    public String org;
    public String app_id;
    public Long timestamp;
    public Integer utc_offset;
    public Boolean error;
    public EnergyParamBaseModel prevEnergyParamPkt = null;

    private static final ObjectMapper mapper = new ObjectMapper();

    public DlgPacketBaseModel(JsonNode packet) {
        if (packet == null)
            return;

        try {
            JsonNode comb_id_jsonNode = packet.get("comb_id");
            if (comb_id_jsonNode != null)
                this.comb_id = comb_id_jsonNode.asText();

            JsonNode meter_id_jsonNode = packet.get("meter_id");
            if (meter_id_jsonNode != null)
                this.meter_id = meter_id_jsonNode.asText();

            JsonNode dlg_id_jsonNode = packet.get("dlg_id");
            if (dlg_id_jsonNode != null)
                this.dlg_id = dlg_id_jsonNode.asText();

            JsonNode zone_id_jsonNode = packet.get("zone_id");
            if (zone_id_jsonNode != null)
                this.zone_id = zone_id_jsonNode.asText();

            JsonNode org_jsonNode = packet.get("org");
            if (org_jsonNode != null)
                this.org = org_jsonNode.asText();

            JsonNode app_id_jsonNode = packet.get("app_id");
            if (app_id_jsonNode != null)
                this.app_id = app_id_jsonNode.asText();

            JsonNode timestamp_jsonNode = packet.get("timestamp");
            if (timestamp_jsonNode != null)
                this.timestamp = timestamp_jsonNode.asLong();

            JsonNode utc_offset_jsonNode = packet.get("utc_offset");
            if (utc_offset_jsonNode != null)
                this.utc_offset = utc_offset_jsonNode.intValue();

            JsonNode error_jsonNode = packet.get("error");
            if (error_jsonNode != null)
                this.error = error_jsonNode.booleanValue();

            JsonNode prePktJsonNode = packet.get("prevEnergyParamPkt");
            if (prePktJsonNode != null) {
                try {
                    this.prevEnergyParamPkt = new EnergyParamBaseModel(prePktJsonNode).getData();
                } catch (Exception e) {
                    System.err.println("prevPktIsNull " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("DLG ingestModel error " + e.getMessage());
        }
    }

    public DlgPacketBaseModel getData() {
        return this;
    }

    public JsonNode toJsonNode() {
        return mapper.valueToTree(this);
    }
}
