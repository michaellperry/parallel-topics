package com.example.kafka.consumer;

import com.example.kafka.common.Purchase;
import com.example.kafka.common.PurchaseSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Processes purchase data using Kafka Streams.
 * 
 * This consumer:
 * 1. Consumes purchase messages from the input topic (key = orderId)
 * 2. Remaps the messages to use SKU as the key
 * 3. Aggregates the quantities by SKU
 * 4. Outputs the results to a KTable and a topic
 */
public class PurchaseProcessor {
    private static final Logger logger = LoggerFactory.getLogger(PurchaseProcessor.class);
    
    private final KafkaStreams streams;
    private final CountDownLatch latch = new CountDownLatch(1);
    
    public PurchaseProcessor(String bootstrapServers, String applicationId, String inputTopic, String outputTopic) {
        // Configure the Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        // Create custom Serdes for Purchase objects
        final Serde<Purchase> purchaseSerde = new PurchaseSerde();
        
        // Use the Purchase serde as the default value serde
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, PurchaseSerde.class.getName());
        
        // Configure internal topics with proper replication factor
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        
        // Configure state store settings
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/" + applicationId);
        
        // Configure processing guarantees
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        
        // Build the Streams topology
        final Topology topology = buildTopology(inputTopic, outputTopic, purchaseSerde);
        logger.info("Topology: {}", topology.describe());
        
        // Create the KafkaStreams instance
        streams = new KafkaStreams(topology, props);
        
        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down streams");
            streams.close();
            latch.countDown();
        }));
    }
    
    /**
     * Builds the Kafka Streams topology.
     * 
     * @param inputTopic The input topic to consume from
     * @param outputTopic The output topic to produce to
     * @param purchaseSerde The Serde for Purchase objects
     * @return The Streams topology
     */
    private Topology buildTopology(String inputTopic, String outputTopic, Serde<Purchase> purchaseSerde) {
        final StreamsBuilder builder = new StreamsBuilder();
        
        // Read from the input topic with orderId as key
        KStream<String, Purchase> purchaseStream = builder.stream(
                inputTopic, Consumed.with(Serdes.String(), purchaseSerde));
        
        // Remap to use SKU as the key instead of orderId
        KStream<String, Purchase> skuKeyedStream = purchaseStream
                .selectKey((orderId, purchase) -> purchase.getSku());
        
        // Aggregate quantities by SKU
        KTable<String, Integer> skuQuantityTable = skuKeyedStream
                .groupByKey()
                .aggregate(
                    () -> 0, // Initial value
                    (sku, purchase, totalQuantity) -> totalQuantity + purchase.getQuantity(),
                    Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("sku-totals-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Integer())
                );
        
        // Output the aggregated results to a topic
        skuQuantityTable.toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));
        
        logger.info("Built topology: Input topic -> Remap key to SKU -> Aggregate by SKU -> Output topic");
        
        return builder.build();
    }
    
    /**
     * Starts the Streams application.
     */
    public void start() {
        logger.info("Starting Streams");
        
        // Clean up local state before starting
        streams.cleanUp();
        
        // Set the uncaught exception handler
        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            logger.error("Uncaught exception in Kafka Streams: ", throwable);
            // You might want to restart the streams or take other actions here
        });
        
        // Set the state listener to monitor state transitions
        streams.setStateListener((newState, oldState) -> {
            logger.info("Kafka Streams state transition from {} to {}", oldState, newState);
        });
        
        // Start the streams
        streams.start();
        
        // Start a background thread to wait for the latch
        Thread streamWaitThread = new Thread(() -> {
            try {
                latch.await();
                logger.info("Streams application has been shut down");
            } catch (InterruptedException e) {
                logger.error("Streams wait thread interrupted", e);
                Thread.currentThread().interrupt();
            }
        });
        streamWaitThread.setDaemon(true);
        streamWaitThread.start();
        
        logger.info("Streams started in background");
    }
    
    /**
     * Waits for the Streams application to be in the RUNNING state.
     * 
     * @param timeoutMs The maximum time to wait in milliseconds
     * @return true if the streams application is in the RUNNING state, false otherwise
     */
    public boolean waitForRunningState(long timeoutMs) {
        logger.info("Waiting for Kafka Streams to be in RUNNING state (timeout: {} ms)", timeoutMs);
        
        final long startTime = System.currentTimeMillis();
        final long endTime = startTime + timeoutMs;
        
        while (System.currentTimeMillis() < endTime) {
            if (streams.state() == KafkaStreams.State.RUNNING) {
                logger.info("Kafka Streams is now in RUNNING state");
                return true;
            }
            
            logger.info("Kafka Streams is in {} state, waiting...", streams.state());
            
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted while waiting for RUNNING state", e);
                return false;
            }
        }
        
        logger.warn("Timed out waiting for Kafka Streams to be in RUNNING state. Current state: {}", streams.state());
        return false;
    }
    
    /**
     * Stops the Streams application.
     */
    public void stop() {
        logger.info("Stopping Streams");
        streams.close();
        latch.countDown();
    }
    
    /**
     * Gets the KafkaStreams instance.
     * 
     * @return The KafkaStreams instance
     */
    public KafkaStreams getStreams() {
        return streams;
    }
}
