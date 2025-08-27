package com.kazam_dlg_ingest.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EventTimeExtractor implements TimestampExtractor {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        try {

            String value = record.value().toString();
            JsonNode rootNode = mapper.readTree(value);

            JsonNode timeJsonNode = rootNode.get("timestamp");
            if (timeJsonNode != null && !timeJsonNode.isNull() && timeJsonNode.isNumber()) {
                long timestampSec = timeJsonNode.asLong();
                long adjustedTimeMs = timestampSec * 1000L;

                // long time = (timeJsonNode.asLong() - (offset * 60)) * 1000L;
                return adjustedTimeMs;
            }

        } catch (JsonProcessingException e) {
            System.err.println("Invalid JSON: " + e.getMessage());
        }

        return partitionTime;
    }
}
