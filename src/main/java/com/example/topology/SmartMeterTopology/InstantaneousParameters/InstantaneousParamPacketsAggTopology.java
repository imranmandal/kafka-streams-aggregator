package com.example.topology.SmartMeterTopology.InstantaneousParameters;

import java.util.ArrayList;
import java.util.List;

import com.example.models.InstantaneousParameterModels.InstantaneousParamBaseModel;
import com.example.models.InstantaneousParameterModels.InstantaneousParameterPacketModel;
import com.example.utils.SmartMeterParamUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class InstantaneousParamPacketsAggTopology {
    private static final ObjectMapper mapper = new ObjectMapper();
    private String[] parameters = new SmartMeterParamUtil().SecureMeterInstantaneousParamsMap;

    public ObjectNode min_max_count = mapper.createObjectNode();
    public List<InstantaneousParameterPacketModel> packets = new ArrayList<>();

    public InstantaneousParamPacketsAggTopology(InstantaneousParamAggTopology agg, InstantaneousParamBaseModel curr) {
        JsonNode jsonCurrData = mapper.valueToTree(curr);

        if (agg == null || agg.packetsAgg == null) {
            for (String param : parameters) {
                if (jsonCurrData.has(param)) {
                    double data = jsonCurrData.get(param).doubleValue();

                    this.min_max_count.put(param + "_min", data);
                    this.min_max_count.put(param + "_max", data);
                    this.min_max_count.put(param + "_count", 1);
                }
            }
        } else {
            JsonNode jsonAggData = mapper.valueToTree(agg.packetsAgg);
            JsonNode min_max_count_agg = jsonAggData.get("min_max_count");

            for (String param : parameters) {
                if (jsonCurrData.has(param)) {
                    double data = jsonCurrData.get(param).doubleValue();

                    JsonNode agg_min = min_max_count_agg != null ? min_max_count_agg.get(param + "_min") : null;
                    double agg_min_value = agg_min != null ? agg_min.doubleValue() : 0;
                    double data_min = agg_min != null
                            ? agg_min_value > data ? data : agg_min_value
                            : data;

                    JsonNode agg_max = min_max_count_agg != null ? min_max_count_agg.get(param + "_max") : null;
                    double agg_max_value = agg_max != null ? agg_max.doubleValue() : 0;
                    double data_max = agg_max != null
                            ? agg_max_value < data ? data : agg_max_value
                            : data;

                    JsonNode agg_count = min_max_count_agg != null ? min_max_count_agg.get(param + "_count") : null;
                    double agg_count_value = agg_count != null ? agg_count.doubleValue() : 0;
                    double data_count = agg_count_value + 1;

                    this.min_max_count.put(param + "_min", data_min);
                    this.min_max_count.put(param + "_max", data_max);
                    this.min_max_count.put(param + "_count", data_count);
                }
            }

            if (agg.packetsAgg.packets != null)
                this.packets.addAll(agg.packetsAgg.packets);
        }

        if (curr != null) {
            InstantaneousParameterPacketModel data = new InstantaneousParameterPacketModel(curr).getData();
            if (data != null) {
                this.packets.add(data);
            }
        }
    }

    @JsonIgnore
    public InstantaneousParamPacketsAggTopology getData() {
        return this;
    }

    @JsonIgnore
    public JsonNode toJsonNode() {
        JsonNode result = mapper.valueToTree(this);
        return result;
    }

    public InstantaneousParamPacketsAggTopology parse(JsonNode aggData) {

        JsonNode min_max_count_agg = aggData.get("min_max_count");
        if (min_max_count_agg != null) {
            for (String param : parameters) {
                JsonNode _min = min_max_count_agg.get(param + "_min");
                JsonNode _max = min_max_count_agg.get(param + "_max");
                JsonNode _count = min_max_count_agg.get(param + "_count");
                if (_min != null
                        && _max != null
                        && _count != null) {

                    this.min_max_count.put(param + "_min",
                            min_max_count_agg.get(param + "_min").doubleValue());
                    this.min_max_count.put(param + "_max",
                            min_max_count_agg.get(param + "_max").doubleValue());
                    this.min_max_count.put(param + "_count",
                            min_max_count_agg.get(param + "_count").doubleValue());
                }
            }
        }

        if (aggData.has("packets") && aggData.get("packets").isArray()) {
            aggData.get("packets").forEach(d -> {
                this.packets.add(new InstantaneousParameterPacketModel(null).parse(d));
            });
        }

        return this;
    }

}