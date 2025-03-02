package com.example.kafka.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application class for the purchase processor.
 */
public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    
    public static void main(String[] args) {
        logger.info("Starting Purchase Processor Application");
        
        // Get configuration from environment variables or use defaults
        String bootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String applicationId = getEnvOrDefault("APPLICATION_ID", "purchase-processor");
        String inputTopic = getEnvOrDefault("INPUT_TOPIC", "purchases");
        String outputTopic = getEnvOrDefault("OUTPUT_TOPIC", "sku-totals");
        
        logger.info("Configuration: bootstrapServers={}, applicationId={}, inputTopic={}, outputTopic={}",
                bootstrapServers, applicationId, inputTopic, outputTopic);
        
        // Create and start the processor
        PurchaseProcessor processor = new PurchaseProcessor(
                bootstrapServers, applicationId, inputTopic, outputTopic);
        
        // Add shutdown hook to gracefully stop the processor
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down processor");
            processor.stop();
        }));
        
        // Start the processor
        processor.start();
        
        logger.info("Processor started and running...");
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
