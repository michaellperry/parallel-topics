package com.example.kafka.consumer;

import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application class for the purchase consumer.
 */
public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    
    public static void main(String[] args) {
        logger.info("Starting Purchase Processor Application");
        
        // Get configuration from environment variables or use defaults
        String bootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String applicationId = getEnvOrDefault("APPLICATION_ID", "purchase-consumer");
        String inputTopic = getEnvOrDefault("INPUT_TOPIC", "purchases");
        String outputTopic = getEnvOrDefault("OUTPUT_TOPIC", "sku-totals");
        int httpPort = Integer.parseInt(getEnvOrDefault("HTTP_PORT", "7000"));
        
        logger.info("Configuration: bootstrapServers={}, applicationId={}, inputTopic={}, outputTopic={}, httpPort={}",
                bootstrapServers, applicationId, inputTopic, outputTopic, httpPort);
        
        // Create the consumer
        PurchaseProcessor consumer = new PurchaseProcessor(
                bootstrapServers, applicationId, inputTopic, outputTopic);
        
        // Create the KTableController
        KTableController controller = new KTableController(
                consumer.getStreams(), "sku-totals-store");
        
        // Start Javalin HTTP server
        Javalin app = Javalin.create(config -> {
            config.plugins.enableCors(cors -> cors.add(it -> it.anyHost()));
        }).start(httpPort);
        
        // Register endpoint
        app.get("/sku-totals", ctx -> {
            logger.info("Received request for /sku-totals");
            ctx.json(controller.getSkuTotals());
        });
        
        // Add health check endpoint
        app.get("/health", ctx -> {
            logger.info("Received health check request");
            ctx.result("OK");
        });
        
        // Add shutdown hook to gracefully stop the consumer and HTTP server
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down consumer and HTTP server");
            app.stop();
            consumer.stop();
        }));
        
        // Start the consumer
        consumer.start();
        
        logger.info("Processor and HTTP server started and running on port {}...", httpPort);
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
