package com.example.kafka.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Deserializes JSON data from Kafka into Purchase objects.
 */
public class PurchaseDeserializer implements Deserializer<Purchase> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to configure
    }

    @Override
    public Purchase deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        
        try {
            return objectMapper.readValue(data, Purchase.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing JSON to Purchase", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
