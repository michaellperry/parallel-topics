package com.example.kafka.consumer;

import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

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
        
        // Start the consumer
        logger.info("Starting Kafka Streams application");
        consumer.start();
        
        // Create the KTableController
        KTableController controller = new KTableController(
                consumer.getStreams(), "sku-totals-store");
        
        // Wait for the Kafka Streams application to be in the RUNNING state (with a timeout)
        boolean streamsReady = consumer.waitForRunningState(30000); // 30 seconds timeout
        if (streamsReady) {
            logger.info("Kafka Streams application is now in RUNNING state, state stores should be available");
        } else {
            logger.warn("Kafka Streams application did not reach RUNNING state within timeout, " +
                    "state stores may not be immediately available");
        }
        
        // Start Javalin HTTP server
        Javalin app = Javalin.create(config -> {
            config.plugins.enableCors(cors -> cors.add(it -> it.anyHost()));
        }).start(httpPort);
        
        // Register endpoint with improved error handling
        app.get("/sku-totals", ctx -> {
            logger.info("Received request for /sku-totals");
            
            // Check if the state store is ready
            if (!controller.isStateStoreReady()) {
                logger.warn("State store is not ready yet, returning 503 Service Unavailable");
                ctx.status(503); // Service Unavailable
                ctx.json(Map.of(
                    "error", "Service is starting up, please try again later",
                    "status", "NOT_READY"
                ));
                return;
            }
            
            // Get the SKU totals
            Map<String, Integer> skuTotals = controller.getSkuTotals();
            ctx.json(skuTotals);
        });
        
        // Add enhanced health check endpoint that also reports Kafka Streams state
        app.get("/health", ctx -> {
            logger.info("Received health check request");
            
            boolean storeReady = controller.isStateStoreReady();
            String streamsState = consumer.getStreams().state().name();
            
            ctx.json(Map.of(
                "status", "UP",
                "kafka_streams", Map.of(
                    "state", streamsState,
                    "state_store_ready", storeReady
                )
            ));
        });
        
        // Add a status endpoint for Kafka Streams
        app.get("/status", ctx -> {
            logger.info("Received status request");
            
            boolean storeReady = controller.isStateStoreReady();
            String streamsState = consumer.getStreams().state().name();
            
            ctx.json(Map.of(
                "kafka_streams_state", streamsState,
                "state_store_ready", storeReady,
                "state_store_name", "sku-totals-store",
                "application_id", applicationId,
                "bootstrap_servers", bootstrapServers,
                "input_topic", inputTopic,
                "output_topic", outputTopic
            ));
        });
        
        // Add a wait endpoint that blocks until the state store is ready
        app.get("/wait-for-store", ctx -> {
            logger.info("Received wait-for-store request");
            
            // Get the timeout parameter (default to 30 seconds)
            int timeoutMs = ctx.queryParamAsClass("timeout", Integer.class).getOrDefault(30000);
            
            // Check if the state store is already ready
            if (controller.isStateStoreReady()) {
                logger.info("State store is already ready");
                ctx.json(Map.of(
                    "status", "READY",
                    "message", "State store is ready"
                ));
                return;
            }
            
            // Wait for the state store to be ready
            logger.info("Waiting for state store to be ready (timeout: {} ms)", timeoutMs);
            long startTime = System.currentTimeMillis();
            long endTime = startTime + timeoutMs;
            
            while (System.currentTimeMillis() < endTime) {
                if (controller.isStateStoreReady()) {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    logger.info("State store is now ready (waited {} ms)", elapsedTime);
                    ctx.json(Map.of(
                        "status", "READY",
                        "message", "State store is ready",
                        "wait_time_ms", elapsedTime
                    ));
                    return;
                }
                
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Wait interrupted", e);
                    break;
                }
            }
            
            // Timed out waiting for the state store to be ready
            logger.warn("Timed out waiting for state store to be ready");
            ctx.status(503); // Service Unavailable
            ctx.json(Map.of(
                "status", "TIMEOUT",
                "message", "Timed out waiting for state store to be ready",
                "kafka_streams_state", consumer.getStreams().state().name()
            ));
        });
        
        // Add shutdown hook to gracefully stop the consumer and HTTP server
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down consumer and HTTP server");
            app.stop();
            consumer.stop();
        }));
        
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
