package com.example.topology.SmartMeterLopology.EnergyParameters.Aggregators;

import com.example.models.EnergyParameterModels.EnergyParamBaseModel;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EnergyParamAggLogsTopology {
    public double energy_active_import = 0;
    public double energy_active_import_min = Double.MAX_VALUE;
    public double energy_active_import_max = Double.MIN_VALUE;
    public int energy_active_import_count = 0;
    public double energy_active_export = 0;
    public double energy_active_export_min = Double.MAX_VALUE;
    public double energy_active_export_max = Double.MIN_VALUE;
    public int energy_active_export_count = 0;

    public EnergyParamAggLogsTopology(EnergyParamAgg acc, EnergyParamBaseModel curr) {
        if (acc == null)
            return;

        if (acc.aggregatedLogs == null) {
            acc.aggregatedLogs = new EnergyParamAggLogsTopology(null, null).getData();
        }

        // import
        this.energy_active_import = acc.aggregatedLogs.energy_active_import + curr.energy_active_import_delta;

        if (curr.energy_active_import > 0) {
            this.energy_active_import_count = acc.aggregatedLogs.energy_active_import_count + 1;
        } else {
            this.energy_active_import_count = acc.aggregatedLogs.energy_active_import_count;
        }

        if (acc.aggregatedLogs.energy_active_import_min > curr.energy_active_import) {
            this.energy_active_import_min = curr.energy_active_import;
        } else {
            this.energy_active_import_min = acc.aggregatedLogs.energy_active_import_min;
        }

        if (acc.aggregatedLogs.energy_active_import_max < curr.energy_active_import) {
            this.energy_active_import_max = curr.energy_active_import;
        } else {
            this.energy_active_import_max = acc.aggregatedLogs.energy_active_import_max;
        }

        // export
        this.energy_active_export = acc.aggregatedLogs.energy_active_export + curr.energy_active_export_delta;

        if (curr.energy_active_export > 0) {
            this.energy_active_export_count = acc.aggregatedLogs.energy_active_export_count + 1;
        } else {
            this.energy_active_export_count = acc.aggregatedLogs.energy_active_export_count;
        }

        if (acc.aggregatedLogs.energy_active_export_min > curr.energy_active_export) {
            this.energy_active_export_min = curr.energy_active_export;
        } else {
            this.energy_active_export_min = acc.aggregatedLogs.energy_active_export_min;
        }

        if (acc.aggregatedLogs.energy_active_export_max < curr.energy_active_export) {
            this.energy_active_export_max = curr.energy_active_export;
        } else {
            this.energy_active_export_max = acc.aggregatedLogs.energy_active_export_max;
        }

    }

    public EnergyParamAggLogsTopology parse(JsonNode packet) {
        if (packet == null)
            return this;

        if (packet.has("energy_active_import"))
            this.energy_active_import = packet.get("energy_active_import").doubleValue();

        if (packet.has("energy_active_import_min"))
            this.energy_active_import_min = packet.get("energy_active_import_min").doubleValue();

        if (packet.has("energy_active_import_max"))
            this.energy_active_import_max = packet.get("energy_active_import_max").doubleValue();

        if (packet.has("energy_active_import_count"))
            this.energy_active_import_count = packet.get("energy_active_import_count").intValue();

        if (packet.has("energy_active_export"))
            this.energy_active_export = packet.get("energy_active_export").doubleValue();

        if (packet.has("energy_active_export_min"))
            this.energy_active_export_min = packet.get("energy_active_export_min").doubleValue();

        if (packet.has("energy_active_export_max"))
            this.energy_active_export_max = packet.get("energy_active_export_max").doubleValue();

        if (packet.has("energy_active_export_count"))
            this.energy_active_export_count = packet.get("energy_active_export_count").intValue();

        return this;
    }

    @JsonIgnore
    public JsonNode toJsonNode() {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.valueToTree(this);
    }

    @JsonIgnore
    public EnergyParamAggLogsTopology getData() {
        return this;
    }
}
