package com.example.kafka.common;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Deserializes plain string data from Kafka into Integer objects.
 * This deserializer converts string representations of integers directly to Integer objects
 * without expecting them to be wrapped in JSON objects.
 */
public class PlainIntegerDeserializer implements Deserializer<Integer> {

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
            // Convert the string representation directly to an Integer
            String strValue = new String(data, StandardCharsets.UTF_8);
            return Integer.parseInt(strValue);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing string to Integer", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
