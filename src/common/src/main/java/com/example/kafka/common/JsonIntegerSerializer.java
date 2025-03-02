package com.example.kafka.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serializes Integer values to JSON for Kafka.
 */
public class JsonIntegerSerializer implements Serializer<Integer> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to configure
    }

    @Override
    public byte[] serialize(String topic, Integer data) {
        if (data == null) {
            return null;
        }
        
        try {
            // Create a JSON object with a "value" field
            return objectMapper.writeValueAsBytes(Map.of("value", data));
        } catch (Exception e) {
            throw new RuntimeException("Error serializing Integer to JSON", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
