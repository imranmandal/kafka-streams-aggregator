package com.example.serder;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.example.models.InstantaneousParameterModels.InstantaneousParamBaseModel;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class InstantaneousParamSerde implements Serde<InstantaneousParamBaseModel> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Serializer<InstantaneousParamBaseModel> serializer() {
        return (topic, data) -> {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                System.err.println("InstantaneousParamSerde serialize error == " + e.getMessage());
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<InstantaneousParamBaseModel> deserializer() {
        return (topic, data) -> {
            try {
                JsonNode json = mapper.readTree(data);
                return new InstantaneousParamBaseModel(json).getData();
            } catch (Exception e) {
                System.err.println("InstantaneousParamSerde deserialize error == " + e.getMessage());
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
