package com.kazam_dlg_ingest.topology.SmartMeterTopology.InstantaneousParameters;

import java.util.HashMap;
import java.util.Map;

import com.kazam_dlg_ingest.models.InstantaneousParameterModels.InstantaneousParamBaseModel;
import com.kazam_dlg_ingest.utils.SmartMeterParamUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;

public class InstantaneousParamAggMinMaxTopology {
    public Map<String, Double> min_max_agg = new HashMap<>();
    private String[] params = new SmartMeterParamUtil().SecureMeterInstantaneousParamsMap;

    public InstantaneousParamAggMinMaxTopology(InstantaneousParamAggTopology acc, InstantaneousParamBaseModel curr) {
        if (acc == null)
            return;

        for (String param : params) {
            try {
                double value = curr.instantaneousParams.get(param).doubleValue();

                // Aggregating field value sum
                this.min_max_agg.put(param,
                        (acc.durationLogsAgg != null
                                ? acc.durationLogsAgg.min_max_agg.get(param).doubleValue()
                                : 0) +
                                value);

                Double prevMin = acc.durationLogsAgg != null
                        ? acc.durationLogsAgg.min_max_agg.get(param + "_min").doubleValue()
                        : null;
                this.min_max_agg.put(param + "_min", prevMin == null ? value : Math.min(prevMin, value));

                Double prevMax = acc.durationLogsAgg != null
                        ? acc.durationLogsAgg.min_max_agg.get(param + "_max").doubleValue()
                        : null;
                this.min_max_agg.put(param + "_max", prevMax == null ? value : Math.max(prevMax, value));

                Double prevCount = acc.durationLogsAgg != null
                        ? acc.durationLogsAgg.min_max_agg.get(param + "_count").doubleValue()
                        : 0;

                // incrementing if value is greater than Zero
                this.min_max_agg.put(param + "_count", value > 0 ? prevCount + 1 : prevCount);

            } catch (Exception e) {
                System.err.println("InstantaneousParamAggMinMaxTopology constructor " + param
                        + " " + e.getMessage());
            }
        }
    }

    @JsonIgnore
    public InstantaneousParamAggMinMaxTopology getData() {
        return this;
    }

    public InstantaneousParamAggMinMaxTopology parse(JsonNode data) {
        for (String param : params) {
            try {
                if (data.has("min_max_agg")) {
                    if (data.get("min_max_agg").has(param)) {
                        this.min_max_agg.put(param, data.get("min_max_agg").get(param).doubleValue());
                    }
                    if (data.get("min_max_agg").has(param + "_min")) {
                        this.min_max_agg.put(param + "_min", data.get("min_max_agg").get(param + "_min").doubleValue());
                    }
                    if (data.get("min_max_agg").has(param + "_max")) {
                        this.min_max_agg.put(param + "_max", data.get("min_max_agg").get(param + "_max").doubleValue());
                    }
                    if (data.get("min_max_agg").has(param + "_count")) {
                        this.min_max_agg.put(param + "_count",
                                data.get("min_max_agg").get(param + "_count").doubleValue());
                    }
                }
            } catch (Exception e) {
                System.err.println("InstantaneousParamAggMinMaxTopology parser " + e.getMessage());
            }
        }

        return this;
    }
}
