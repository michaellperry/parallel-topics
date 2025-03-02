package com.example.kafka.processor;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Controller for accessing KTable state stores.
 * 
 * This controller provides methods to query the state of KTables
 * maintained by the Kafka Streams application.
 */
public class KTableController {
    private static final Logger logger = LoggerFactory.getLogger(KTableController.class);
    
    private final KafkaStreams streams;
    private final String stateStoreName;
    
    /**
     * Creates a new KTableController.
     * 
     * @param streams The KafkaStreams instance
     * @param stateStoreName The name of the state store to query
     */
    public KTableController(KafkaStreams streams, String stateStoreName) {
        this.streams = streams;
        this.stateStoreName = stateStoreName;
    }
    
    /**
     * Gets the current totals for each SKU.
     * 
     * @return A map of SKU to quantity
     */
    public Map<String, Integer> getSkuTotals() {
        logger.info("Querying state store: {}", stateStoreName);
        
        // Use the correct API for querying state stores
        ReadOnlyKeyValueStore<String, Integer> keyValueStore =
            streams.store(StoreQueryParameters.fromNameAndType(
                stateStoreName, 
                QueryableStoreTypes.keyValueStore()));
        
        Map<String, Integer> result = new HashMap<>();
        keyValueStore.all().forEachRemaining(kv -> result.put(kv.key, kv.value));
        
        logger.info("Found {} SKUs in the state store", result.size());
        return result;
    }
}
