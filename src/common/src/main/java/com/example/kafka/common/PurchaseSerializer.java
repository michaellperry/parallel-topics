package com.example.kafka.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serializes Purchase objects to JSON for Kafka.
 */
public class PurchaseSerializer implements Serializer<Purchase> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to configure
    }

    @Override
    public byte[] serialize(String topic, Purchase data) {
        if (data == null) {
            return null;
        }
        
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing Purchase to JSON", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
