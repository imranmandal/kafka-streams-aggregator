package com.kazam_dlg_ingest.topology.SmartMeterTopology.EnergyParameters;

import java.util.ArrayList;
import java.util.List;

import com.kazam_dlg_ingest.models.EnergyParameterModels.EnergyParamBaseModel;
import com.kazam_dlg_ingest.models.EnergyParameterModels.EnergyParameterPacketModel;
import com.kazam_dlg_ingest.utils.SmartMeterParamUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class EnergyParamPacketsAggTopology {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static String[] parameters = new SmartMeterParamUtil().SecureMeterEnergyParamsMap;

    public ObjectNode min_max_count = mapper.createObjectNode();
    public List<EnergyParameterPacketModel> packets = new ArrayList<>();

    public EnergyParamPacketsAggTopology(EnergyParamAggTopology agg, EnergyParamBaseModel curr) {
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

                    JsonNode agg_min = (min_max_count_agg != null && !min_max_count_agg.isNull())
                            ? min_max_count_agg.get(param + "_min")
                            : null;
                    double agg_min_value = (agg_min != null && !agg_min.isNull()) ? agg_min.doubleValue() : 0;
                    double data_min = (agg_min != null && !agg_min.isNull())
                            ? agg_min_value > data ? data : agg_min_value
                            : data;

                    JsonNode agg_max = (min_max_count_agg != null && !min_max_count_agg.isNull())
                            ? min_max_count_agg.get(param + "_max")
                            : null;
                    double agg_max_value = (agg_max != null && !agg_max.isNull()) ? agg_max.doubleValue() : 0;
                    double data_max = (agg_max != null && !agg_max.isNull())
                            ? agg_max_value < data ? data : agg_max_value
                            : data;

                    JsonNode agg_count = (min_max_count_agg != null && !min_max_count_agg.isNull())
                            ? min_max_count_agg.get(param + "_count")
                            : null;
                    double agg_count_value = (agg_count != null && !agg_count.isNull()) ? agg_count.doubleValue() : 0;
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

    public EnergyParamPacketsAggTopology parse(JsonNode aggData) {

        JsonNode min_max_count_agg = aggData.get("min_max_count");
        if (min_max_count_agg != null && !min_max_count_agg.isNull()) {
            for (String param : parameters) {
                JsonNode _min = min_max_count_agg.get(param + "_min");
                JsonNode _max = min_max_count_agg.get(param + "_max");
                JsonNode _count = min_max_count_agg.get(param + "_count");
                if ((_min != null && !_min.isNull())
                        && (_max != null && !_max.isNull())
                        && (_count != null && !_count.isNull())) {

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
                this.packets.add(new EnergyParameterPacketModel(null).parse(d));
            });
        }

        return this;
    }

}