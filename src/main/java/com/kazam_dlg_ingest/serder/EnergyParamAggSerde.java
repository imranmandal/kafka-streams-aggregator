package com.kazam_dlg_ingest.serder;

import com.kazam_dlg_ingest.DlgIngestAggregatorApp.AggregatorTopology;
import com.kazam_dlg_ingest.topology.SmartMeterTopology.EnergyParameters.EnergyParamAggTopology;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class EnergyParamAggSerde implements Serde<EnergyParamAggTopology> {
    private final ObjectMapper mapper = new ObjectMapper();
    AggregatorTopology[] topologyStages;

    public EnergyParamAggSerde(AggregatorTopology[] topologyStages) {
        this.topologyStages = topologyStages;
    }

    @Override
    public Serializer<EnergyParamAggTopology> serializer() {
        return (topic, data) -> {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                System.err.println("EnergyParamAgg serialize error == " + e.getMessage());
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<EnergyParamAggTopology> deserializer() {
        return (topic, data) -> {
            try {
                JsonNode jsonNode = mapper.readTree(data);
                return new EnergyParamAggTopology(null, null, this.topologyStages).parse(jsonNode);
                // return mapper.readValue(data, EnergyParamAgg.class);
            } catch (Exception e) {
                System.err.println("EnergyParamAgg deserialize error == " + e.getMessage());
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}
