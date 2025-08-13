package com.example.serder;

import com.example.DlgIngestAggregatorApp.AggregatorTopology;
import com.example.topology.SmartMeterLopology.EnergyParameters.Aggregators.EnergyParamAgg;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class EnergyParamAggSerde implements Serde<EnergyParamAgg> {
    private final ObjectMapper mapper = new ObjectMapper();
    AggregatorTopology[] topologyStages;

    public EnergyParamAggSerde(AggregatorTopology[] topologyStages) {
        this.topologyStages = topologyStages;
    }

    @Override
    public Serializer<EnergyParamAgg> serializer() {
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
    public Deserializer<EnergyParamAgg> deserializer() {
        return (topic, data) -> {
            try {
                JsonNode jsonNode = mapper.readTree(data);
                return new EnergyParamAgg(null, null, this.topologyStages).parse(jsonNode);
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
