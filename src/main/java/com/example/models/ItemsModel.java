package com.example.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ItemsModel {
    // public String id;
    public String org;
    public String item;
    public int count;
    public String timestamp;
    public int price;

    // // Required for Kafka deserialization
    // public ItemsModel() {
    // }

    public ItemsModel(JsonNode packet) {
        // this.id = id;
        this.org = packet.get("org").asText();
        this.item = packet.get("item").asText();
        this.count = packet.get("count").asInt();
        this.timestamp = packet.get("timestamp").asText();
        this.price = packet.get("price").asInt();
    }

    public JsonNode toJsonNode() {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.valueToTree(this);
    }

}
