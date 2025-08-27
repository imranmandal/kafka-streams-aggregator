package com.kazam_dlg_ingest.serder;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.kazam_dlg_ingest.models.MeterUptimeModels.MeterUptimeBaseModel;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MeterUptimeSerde implements Serde<MeterUptimeBaseModel> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Serializer<MeterUptimeBaseModel> serializer() {
        return (topic, data) -> {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                System.err.println("MeterUptimeSerde serialize error == " + e.getMessage());
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<MeterUptimeBaseModel> deserializer() {
        return (topic, data) -> {
            try {
                JsonNode json = mapper.readTree(data);
                MeterUptimeBaseModel meterUptime = new MeterUptimeBaseModel(json).getData();
                return meterUptime;
            } catch (Exception e) {
                System.err.println("MeterUptimeSerde deserialize error == " + e.getMessage());
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
