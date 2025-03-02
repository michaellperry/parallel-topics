package com.example.kafka.consumer;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    private volatile boolean storeReady = false;
    
    // Retry configuration
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 100;
    private static final long MAX_RETRY_DELAY_MS = 1000;
    
    /**
     * Creates a new KTableController.
     * 
     * @param streams The KafkaStreams instance
     * @param stateStoreName The name of the state store to query
     */
    public KTableController(KafkaStreams streams, String stateStoreName) {
        this.streams = streams;
        this.stateStoreName = stateStoreName;
        
        try {
            // Add state listener to track when the streams application is running
            streams.setStateListener((newState, oldState) -> {
                logger.info("Kafka Streams state transition from {} to {}", oldState, newState);
                if (newState == State.RUNNING) {
                    this.storeReady = true;
                    logger.info("Kafka Streams is now RUNNING, state stores should be available");
                } else {
                    this.storeReady = false;
                }
            });
        } catch (IllegalStateException e) {
            // If the streams application is already started, we can't set a state listener
            // In this case, we'll just check the state directly
            logger.warn("Could not set state listener: {}. Will check state directly.", e.getMessage());
            if (streams.state() == State.RUNNING) {
                this.storeReady = true;
                logger.info("Kafka Streams is already RUNNING, state stores should be available");
            }
        }
    }
    
    /**
     * Checks if the state store is available.
     * 
     * @return true if the state store is available, false otherwise
     */
    public boolean isStateStoreReady() {
        if (streams.state() != State.RUNNING) {
            return false;
        }
        
        try {
            // Try to access the store to verify it's available
            streams.store(StoreQueryParameters.fromNameAndType(
                stateStoreName, 
                QueryableStoreTypes.keyValueStore()));
            return true;
        } catch (InvalidStateStoreException e) {
            return false;
        }
    }
    
    /**
     * Gets the current totals for each SKU.
     * 
     * @return A map of SKU to quantity, or an empty map if the state store is not available
     */
    public Map<String, Integer> getSkuTotals() {
        logger.info("Querying state store: {}", stateStoreName);
        
        // Check if the streams instance is in the RUNNING state
        if (streams.state() != State.RUNNING) {
            logger.warn("Cannot query state store because the stream is {}, not RUNNING", streams.state());
            return new HashMap<>();
        }
        
        return getSkuTotalsWithRetry(0, INITIAL_RETRY_DELAY_MS);
    }
    
    /**
     * Gets the current totals for each SKU with retry logic.
     * 
     * @param retryCount The current retry count
     * @param delayMs The delay before the next retry in milliseconds
     * @return A map of SKU to quantity, or an empty map if the state store is not available after retries
     */
    private Map<String, Integer> getSkuTotalsWithRetry(int retryCount, long delayMs) {
        try {
            // Use the correct API for querying state stores
            ReadOnlyKeyValueStore<String, Integer> keyValueStore =
                streams.store(StoreQueryParameters.fromNameAndType(
                    stateStoreName, 
                    QueryableStoreTypes.keyValueStore()));
            
            Map<String, Integer> result = new HashMap<>();
            keyValueStore.all().forEachRemaining(kv -> result.put(kv.key, kv.value));
            
            logger.info("Found {} SKUs in the state store", result.size());
            return result;
        } catch (InvalidStateStoreException e) {
            if (retryCount < MAX_RETRIES) {
                // Calculate next retry delay with exponential backoff (capped at MAX_RETRY_DELAY_MS)
                long nextDelayMs = Math.min(delayMs * 2, MAX_RETRY_DELAY_MS);
                
                logger.warn("State store is not yet available: {}. Retrying in {} ms (retry {}/{})", 
                    e.getMessage(), delayMs, retryCount + 1, MAX_RETRIES);
                
                try {
                    TimeUnit.MILLISECONDS.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.error("Retry interrupted", ie);
                }
                
                return getSkuTotalsWithRetry(retryCount + 1, nextDelayMs);
            } else {
                logger.warn("State store is not available after {} retries: {}", MAX_RETRIES, e.getMessage());
                return new HashMap<>();
            }
        }
    }
}
