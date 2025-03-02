package com.example.kafka.processor;

import com.example.kafka.common.Purchase;
import com.example.kafka.common.PurchaseDeserializer;
import com.example.kafka.common.PurchaseSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Processes purchase data using Kafka Streams.
 * 
 * This processor:
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
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        // Create custom Serdes for Purchase objects
        final Serde<Purchase> purchaseSerde = Serdes.serdeFrom(
                new PurchaseSerializer(), new PurchaseDeserializer());
        
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
                    Materialized.with(Serdes.String(), Serdes.Integer())
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
        streams.start();
        
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Streams interrupted", e);
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Stops the Streams application.
     */
    public void stop() {
        logger.info("Stopping Streams");
        streams.close();
        latch.countDown();
    }
}
