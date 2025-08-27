package com.kazam_dlg_ingest.topology.SmartMeterTopology.EnergyParameters;

import java.util.HashMap;
import java.util.Map;

import com.kazam_dlg_ingest.models.EnergyParameterModels.EnergyParamBaseModel;
import com.kazam_dlg_ingest.utils.SmartMeterParamUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class EnergyParamAggMinMaxTopology {
    public Map<String, Double> min_max_agg = new HashMap<>();

    // public double energy_active_import = 0;
    // public double energy_active_import_min = Double.MAX_VALUE;
    // public double energy_active_import_max = Double.MIN_VALUE;
    // public int energy_active_import_count = 0;
    // public double energy_active_export = 0;
    // public double energy_active_export_min = Double.MAX_VALUE;
    // public double energy_active_export_max = Double.MIN_VALUE;
    // public int energy_active_export_count = 0;

    private SmartMeterParamUtil parameters = new SmartMeterParamUtil();

    public EnergyParamAggMinMaxTopology(EnergyParamAggTopology acc, EnergyParamBaseModel curr) {
        if (acc == null)
            return;

        if (acc.durationLogsAgg == null) {
            acc.durationLogsAgg = new EnergyParamAggMinMaxTopology(null, null).getData();
        }

        ObjectNode accDataJson = (ObjectNode) acc.toJsonNode();
        ObjectNode currDataJson = (ObjectNode) curr.toJsonNode();
        JsonNode accDurationLogAggJsonNode = accDataJson.get("durationLogsAgg").get("min_max_agg");

        for (String param : parameters.SecureMeterEnergyParamsMap) {
            if (!currDataJson.has(param))
                continue;

            Double accParamValue = (accDurationLogAggJsonNode != null && !accDurationLogAggJsonNode.get(param).isNull())
                    ? accDurationLogAggJsonNode.get(param).asDouble()
                    : 0;
            Double accParamCount = (accDurationLogAggJsonNode != null
                    && !accDurationLogAggJsonNode.get(param + "_count").isNull())
                            ? accDurationLogAggJsonNode.get(param + "_count").asDouble()
                            : 0;

            /* Null value assigned to avoid Zero to be considered as the Min or Max value */
            Double accParamMinValue = (accDurationLogAggJsonNode != null
                    && !accDurationLogAggJsonNode.get(param + "_min").isNull())
                            ? accDurationLogAggJsonNode.get(param + "_min").asDouble()
                            : null;
            Double accParamMaxValue = (accDurationLogAggJsonNode != null
                    && !accDurationLogAggJsonNode.get(param + "_max").isNull())
                            ? accDurationLogAggJsonNode.get(param + "_max").asDouble()
                            : null;

            Double currParamValue = (currDataJson != null && !currDataJson.get(param).isNull()
                    && currDataJson.get(param).isNumber())
                            ? currDataJson.get(param).asDouble()
                            : 0;
            Double currParamDeltaValue = (currDataJson != null && !currDataJson.get(param + "_delta").isNull()
                    && currDataJson.get(param + "_delta").isNumber())
                            ? currDataJson.get(param + "_delta").asDouble()
                            : 0;

            JsonNode currPrevEnergyParamPkt = currDataJson.get("prevEnergyParamPkt");

            Double currPrevEnergyParamPktParamValue = (currPrevEnergyParamPkt != null
                    && !currPrevEnergyParamPkt.isNull()
                    && currPrevEnergyParamPkt.get(param) != null && !currPrevEnergyParamPkt.get(param).isNull())
                            ? currPrevEnergyParamPkt.get(param).asDouble()
                            : null;

            // Aggregate sum
            this.min_max_agg.put(param, accParamValue + currParamDeltaValue);

            if (currDataJson.has(param) && currParamValue > 0) {
                this.min_max_agg.put(param + "_count", accParamCount + 1);
            } else {
                this.min_max_agg.put(param + "_count", accParamCount);
            }

            if (accParamMinValue == null) {
                if (currPrevEnergyParamPktParamValue != null) {
                    this.min_max_agg.put(param + "_min", currPrevEnergyParamPktParamValue);
                } else {
                    this.min_max_agg.put(param + "_min", currParamValue);
                }
            } else if (accParamMinValue > currParamValue) {
                this.min_max_agg.put(param + "_min", currParamValue);
            } else {
                this.min_max_agg.put(param + "_min", accParamMinValue);
            }

            if (accParamMaxValue == null || accParamMaxValue < currParamValue) {
                this.min_max_agg.put(param + "_max", currParamValue);
            } else {
                this.min_max_agg.put(param + "_max", accParamMaxValue);
            }

        }

        // // Aggregate import sum
        // this.energy_active_import = acc.durationLogsAgg.energy_active_import +
        // curr.energy_active_import_delta;

        // if (curr.energy_active_import > 0) {
        // this.energy_active_import_count =
        // acc.durationLogsAgg.energy_active_import_count + 1;
        // } else {
        // this.energy_active_import_count =
        // acc.durationLogsAgg.energy_active_import_count;
        // }

        // if (curr.prevEnergyParamPkt != null
        // && acc.durationLogsAgg.energy_active_import_min >
        // curr.prevEnergyParamPkt.energy_active_import) {
        // // checking prevPacket import value
        // this.energy_active_import_min = curr.prevEnergyParamPkt.energy_active_import;
        // } else if (acc.durationLogsAgg.energy_active_import_min >
        // curr.energy_active_import) {
        // this.energy_active_import_min = curr.energy_active_import;
        // } else {
        // this.energy_active_import_min = acc.durationLogsAgg.energy_active_import_min;
        // }

        // if (acc.durationLogsAgg.energy_active_import_max < curr.energy_active_import)
        // {
        // this.energy_active_import_max = curr.energy_active_import;
        // } else {
        // this.energy_active_import_max = acc.durationLogsAgg.energy_active_import_max;
        // }

        // // Aggregate export sum
        // this.energy_active_export = acc.durationLogsAgg.energy_active_export +
        // curr.energy_active_export_delta;

        // if (curr.energy_active_export > 0) {
        // this.energy_active_export_count =
        // acc.durationLogsAgg.energy_active_export_count + 1;
        // } else {
        // this.energy_active_export_count =
        // acc.durationLogsAgg.energy_active_export_count;
        // }

        // if (curr.prevEnergyParamPkt != null
        // && acc.durationLogsAgg.energy_active_export_min >
        // curr.prevEnergyParamPkt.energy_active_export) {
        // // checking prevPacket export value
        // this.energy_active_export_min = curr.prevEnergyParamPkt.energy_active_export;
        // } else if (acc.durationLogsAgg.energy_active_export_min >
        // curr.energy_active_export) {
        // this.energy_active_export_min = curr.energy_active_export;
        // } else {
        // this.energy_active_export_min = acc.durationLogsAgg.energy_active_export_min;
        // }

        // if (acc.durationLogsAgg.energy_active_export_max < curr.energy_active_export)
        // {
        // this.energy_active_export_max = curr.energy_active_export;
        // } else {
        // this.energy_active_export_max = acc.durationLogsAgg.energy_active_export_max;
        // }

    }

    public EnergyParamAggMinMaxTopology parse(JsonNode packet) {
        if (packet == null)
            return this;

        for (String param : parameters.SecureMeterEnergyParamsMap) {
            String count_key = param + "_count";
            String min_key = param + "_min";
            String max_key = param + "_max";

            if (packet.has(param))
                this.min_max_agg.put(param, packet.get(param).asDouble());
            if (packet.has(count_key))
                this.min_max_agg.put(count_key, packet.get(count_key).asDouble());
            if (packet.has(min_key))
                this.min_max_agg.put(min_key, packet.get(min_key).asDouble());
            if (packet.has(max_key))
                this.min_max_agg.put(max_key, packet.get(max_key).asDouble());
        }

        // if (packet.has("energy_active_import"))
        // this.energy_active_import = packet.get("energy_active_import").doubleValue();

        // if (packet.has("energy_active_import_min"))
        // this.energy_active_import_min =
        // packet.get("energy_active_import_min").doubleValue();

        // if (packet.has("energy_active_import_max"))
        // this.energy_active_import_max =
        // packet.get("energy_active_import_max").doubleValue();

        // if (packet.has("energy_active_import_count"))
        // this.energy_active_import_count =
        // packet.get("energy_active_import_count").intValue();

        // if (packet.has("energy_active_export"))
        // this.energy_active_export = packet.get("energy_active_export").doubleValue();

        // if (packet.has("energy_active_export_min"))
        // this.energy_active_export_min =
        // packet.get("energy_active_export_min").doubleValue();

        // if (packet.has("energy_active_export_max"))
        // this.energy_active_export_max =
        // packet.get("energy_active_export_max").doubleValue();

        // if (packet.has("energy_active_export_count"))
        // this.energy_active_export_count =
        // packet.get("energy_active_export_count").intValue();

        return this;
    }

    @JsonIgnore
    public JsonNode toJsonNode() {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.valueToTree(this);
    }

    @JsonIgnore
    public EnergyParamAggMinMaxTopology getData() {
        return this;
    }
}
