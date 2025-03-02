package com.example.kafka.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Custom Serde implementation for Integer objects that serializes to plain string representation.
 * This class combines PlainIntegerSerializer and PlainIntegerDeserializer into a single Serde.
 */
public class PlainIntegerSerde implements Serde<Integer> {
    private final PlainIntegerSerializer serializer = new PlainIntegerSerializer();
    private final PlainIntegerDeserializer deserializer = new PlainIntegerDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<Integer> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Integer> deserializer() {
        return deserializer;
    }
}
