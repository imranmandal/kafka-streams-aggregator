package com.example.serder;

import com.example.DlgIngestAggregatorApp.AggregatorTopology;
import com.example.topology.SmartMeterTopology.InstantaneousParameters.InstantaneousParamAggTopology;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class InstantaneousParamAggSerde implements Serde<InstantaneousParamAggTopology> {
    private final ObjectMapper mapper = new ObjectMapper();
    AggregatorTopology[] topologyStages;

    public InstantaneousParamAggSerde(AggregatorTopology[] topologyStages) {
        this.topologyStages = topologyStages;
    }

    @Override
    public Serializer<InstantaneousParamAggTopology> serializer() {
        return (topic, data) -> {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                System.err.println("InstantaneousParamAggTopology serialize error == " + e.getMessage());
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<InstantaneousParamAggTopology> deserializer() {
        return (topic, data) -> {
            try {
                JsonNode jsonNode = mapper.readTree(data);
                return new InstantaneousParamAggTopology(null, null, this.topologyStages).parse(jsonNode);
                // return mapper.readValue(data, InstantaneousParamAggTopology.class);
            } catch (Exception e) {
                System.err.println("InstantaneousParamAggTopology deserialize error == " + e.getMessage());
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
