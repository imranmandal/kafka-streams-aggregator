package com.example.serder;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.example.models.EnergyParameterModels.EnergyParamBaseModel;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EnergyParamSerde implements Serde<EnergyParamBaseModel> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Serializer<EnergyParamBaseModel> serializer() {
        return (topic, data) -> {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                System.err.println("EnergyParamSerde serialize error == " + e.getMessage());
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<EnergyParamBaseModel> deserializer() {
        return (topic, data) -> {
            try {
                JsonNode json = mapper.readTree(data);
                return new EnergyParamBaseModel(json).getData();
            } catch (Exception e) {
                System.err.println("EnergyParamSerde deserialize error == " + e.getMessage());
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
