package com.example.models.InstantaneousParameterModels;

import java.util.HashMap;
import java.util.Map;

import com.example.models.DlgPacketBaseModel;
import com.example.utils.SmartMeterParamUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class InstantaneousParamBaseModel extends DlgPacketBaseModel {
    public Map<String, Double> instantaneousParams = new HashMap<>();

    // private EnergyParamBaseModel prevPkt = null;
    private static final ObjectMapper mapper = new ObjectMapper();

    private SmartMeterParamUtil params = new SmartMeterParamUtil();

    // public String[] params = {
    // "voltage_r",
    // "voltage_y",
    // "voltage_b",
    // "voltage_avg",
    // "curr_r",
    // "curr_y",
    // "curr_b",
    // "curr_n",
    // "pf_r",
    // "pf_y",
    // "pf_b",
    // "pf_avg",
    // "power_active_r",
    // "power_active_y",
    // "power_active_b",
    // "power_active_sum",
    // "power_reactive_r",
    // "power_reactive_y",
    // "power_reactive_b",
    // "power_reactive_sum",
    // "power_apparent_r",
    // "power_apparent_y",
    // "power_apparent_b",
    // "power_apparent_sum",
    // "freq",
    // "voltage_thd_r",
    // "voltage_thd_y",
    // "voltage_thd_b",
    // "curr_thd_r",
    // "curr_thd_y",
    // "curr_thd_b",
    // };

    public InstantaneousParamBaseModel(JsonNode pkt) {
        super(pkt);

        if (pkt == null)
            return;

        JsonNode instantParamJsonNode = pkt.get("instantaneousParams");
        if (instantParamJsonNode != null) {
            for (String param : params.SecureMeterInstantaneousParamsMap) {
                try {
                    JsonNode paramJsonNode = instantParamJsonNode.get(param);
                    if (paramJsonNode != null) {
                        this.instantaneousParams.put(param, paramJsonNode.asDouble());
                    } else {
                        System.out.println("param not found " + param);
                    }
                } catch (Exception e) {
                    System.err.println("InstantaneousParamModelError " + e.getMessage());
                }
            }
        } else {
            for (String param : params.SecureMeterInstantaneousParamsMap) {
                try {
                    JsonNode paramJsonNode = pkt.get(param);
                    if (paramJsonNode != null) {
                        this.instantaneousParams.put(param, paramJsonNode.asDouble());
                    } else {
                        System.out.println("param not found " + param);
                    }
                } catch (Exception e) {
                    System.err.println("InstantaneousParamModelError " + e.getMessage());
                }
            }

        }

    }

    @JsonIgnore
    public InstantaneousParamBaseModel getData() {
        return this;
    }

    public Map<String, Double> getParams() {
        return this.instantaneousParams;
    }

    @Override
    public JsonNode toJsonNode() {
        return mapper.valueToTree(this.instantaneousParams);
    }
}
