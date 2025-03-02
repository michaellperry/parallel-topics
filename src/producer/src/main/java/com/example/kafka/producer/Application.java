package com.example.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application class for the purchase producer.
 */
public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    
    public static void main(String[] args) {
        logger.info("Starting Purchase Producer Application");
        
        // Get configuration from environment variables or use defaults
        String bootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = getEnvOrDefault("TOPIC_NAME", "purchases");
        long publishIntervalMs = Long.parseLong(getEnvOrDefault("PUBLISH_INTERVAL_MS", "1000"));
        
        logger.info("Configuration: bootstrapServers={}, topic={}, publishIntervalMs={}",
                bootstrapServers, topic, publishIntervalMs);
        
        // Create and start the producer
        PurchaseProducer producer = new PurchaseProducer(bootstrapServers, topic);
        
        // Add shutdown hook to gracefully stop the producer
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down producer");
            producer.stop();
        }));
        
        // Start generating purchase data
        producer.start(publishIntervalMs);
        
        logger.info("Producer started and running...");
    }
    
    /**
     * Gets an environment variable value or returns a default if not set.
     * 
     * @param name The name of the environment variable
     * @param defaultValue The default value to use if the environment variable is not set
     * @return The environment variable value or the default
     */
    private static String getEnvOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
}
