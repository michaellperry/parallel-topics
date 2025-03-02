package com.example.kafka.common;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Serializes Integer values to plain string representation for Kafka.
 * This serializer converts integers directly to their string representation
 * without wrapping them in JSON objects.
 */
public class PlainIntegerSerializer implements Serializer<Integer> {

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
            // Convert the integer directly to its string representation
            return data.toString().getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing Integer to string", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
