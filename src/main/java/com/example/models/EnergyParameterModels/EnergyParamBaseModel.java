package com.example.models.EnergyParameterModels;

import com.example.models.DlgPacketBaseModel;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EnergyParamBaseModel extends DlgPacketBaseModel {
    public double energy_active_export_delta = 0;
    public double energy_active_import_delta = 0;
    public double energy_active_export = 0;
    public double energy_active_import = 0;
    // private EnergyParamBaseModel prevPkt = null;
    private static final ObjectMapper mapper = new ObjectMapper();

    public EnergyParamBaseModel(JsonNode pkt) {
        super(pkt);

        try {

            JsonNode energyExportJsonNode = pkt.get("energy_active_export");
            if (energyExportJsonNode != null) {
                this.energy_active_export = energyExportJsonNode.doubleValue();
            }

            JsonNode energyImportJsonNode = pkt.get("energy_active_import");
            if (energyImportJsonNode != null) {
                this.energy_active_import = energyImportJsonNode.doubleValue();
            }

            if (this.prevEnergyParamPkt != null) {
                this.energy_active_export_delta = this.energy_active_export
                        - this.prevEnergyParamPkt.energy_active_export;
                this.energy_active_import_delta = this.energy_active_import
                        - this.prevEnergyParamPkt.energy_active_import;

            }

        } catch (Exception e) {
            System.err.println("EnergyParamModelError " + e.getMessage());
        }
    }

    @JsonIgnore
    public EnergyParamBaseModel getData() {
        return this;
    }

    @Override
    public JsonNode toJsonNode() {
        return mapper.valueToTree(this);
    }
}
