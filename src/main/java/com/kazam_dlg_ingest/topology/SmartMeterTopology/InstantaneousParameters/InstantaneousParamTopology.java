package com.kazam_dlg_ingest.topology.SmartMeterTopology.InstantaneousParameters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class InstantaneousParamTopology {
    ObjectMapper mapper = new ObjectMapper();

    public String[] MeterInstantaneousParamsMap = {
            "voltage_r",
            "voltage_y",
            "voltage_b",
            "voltage_avg",
            "curr_r",
            "curr_y",
            "curr_b",
            "curr_n",
            "pf_r",
            "pf_y",
            "pf_b",
            "pf_avg",
            "power_active_r",
            "power_active_y",
            "power_active_b",
            "power_active_sum",
            "power_reactive_r",
            "power_reactive_y",
            "power_reactive_b",
            "power_reactive_sum",
            "power_apparent_r",
            "power_apparent_y",
            "power_apparent_b",
            "power_apparent_sum",
            "freq",
            "voltage_thd_r",
            "voltage_thd_y",
            "voltage_thd_b",
            "curr_thd_r",
            "curr_thd_y",
            "curr_thd_b",
    };

    public InstantaneousParamTopology(JsonNode packet) {
        for (int i = 0; i < MeterInstantaneousParamsMap.length; i++) {
            if (packet.has(MeterInstantaneousParamsMap[i])) {
                // mapper.
            }
        }
    }

    public JsonNode toJsonNode() {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.valueToTree(this);
    }
}
