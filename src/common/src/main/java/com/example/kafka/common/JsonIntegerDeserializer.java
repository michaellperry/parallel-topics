package com.example.kafka.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Deserializes JSON data from Kafka into Integer objects.
 */
public class JsonIntegerDeserializer implements Deserializer<Integer> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to configure
    }

    @Override
    public Integer deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        
        try {
            // Read the "value" field from the JSON object
            Map<String, Integer> map = objectMapper.readValue(data, 
                new TypeReference<Map<String, Integer>>() {});
            return map.get("value");
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing JSON to Integer", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
