package com.example.kafka.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Custom Serde implementation for Purchase objects.
 * This class combines PurchaseSerializer and PurchaseDeserializer into a single Serde.
 */
public class PurchaseSerde implements Serde<Purchase> {
    private final PurchaseSerializer serializer = new PurchaseSerializer();
    private final PurchaseDeserializer deserializer = new PurchaseDeserializer();

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
    public Serializer<Purchase> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Purchase> deserializer() {
        return deserializer;
    }
}
