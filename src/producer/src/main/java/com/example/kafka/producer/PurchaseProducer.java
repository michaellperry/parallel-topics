package com.example.kafka.producer;

import com.example.kafka.common.Purchase;
import com.example.kafka.common.PurchaseSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Generates random purchase data and publishes it to a Kafka topic.
 */
public class PurchaseProducer {
    private static final Logger logger = LoggerFactory.getLogger(PurchaseProducer.class);
    
    private final Producer<String, Purchase> producer;
    private final String topic;
    private final Random random = new Random();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    // Sample data for generating random purchases
    private static final String[] CUSTOMER_IDS = {"C1001", "C1002", "C1003", "C1004", "C1005"};
    private static final String[] SKUS = {"SKU1001", "SKU1002", "SKU1003", "SKU1004", "SKU1005"};
    private static final double[] PRICES = {9.99, 19.99, 4.99, 99.99, 29.99};
    
    public PurchaseProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        
        // Configure the producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PurchaseSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        
        this.producer = new KafkaProducer<>(props);
        
        logger.info("Purchase producer initialized with bootstrap servers: {}, topic: {}", 
                bootstrapServers, topic);
    }
    
    /**
     * Starts generating purchase data at the specified interval.
     * 
     * @param intervalMs The interval in milliseconds between generated purchases
     */
    public void start(long intervalMs) {
        logger.info("Starting purchase data generation with interval: {} ms", intervalMs);
        
        scheduler.scheduleAtFixedRate(() -> {
            try {
                sendRandomPurchase();
            } catch (Exception e) {
                logger.error("Error sending purchase data", e);
            }
        }, 0, intervalMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Generates and sends a random purchase to the Kafka topic.
     */
    private void sendRandomPurchase() {
        String orderId = UUID.randomUUID().toString();
        String customerId = CUSTOMER_IDS[random.nextInt(CUSTOMER_IDS.length)];
        String sku = SKUS[random.nextInt(SKUS.length)];
        int quantity = random.nextInt(5) + 1; // 1-5 items
        double unitPrice = PRICES[random.nextInt(PRICES.length)];
        
        Purchase purchase = new Purchase(orderId, customerId, sku, quantity, unitPrice);
        
        // Send the purchase with the order ID as the key
        ProducerRecord<String, Purchase> record = new ProducerRecord<>(topic, orderId, purchase);
        
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                logger.info("Sent purchase: orderId={}, customerId={}, sku={}, quantity={}, unitPrice={}, extendedPrice={}",
                        purchase.getOrderId(), purchase.getCustomerId(), purchase.getSku(), 
                        purchase.getQuantity(), purchase.getUnitPrice(), purchase.getExtendedPrice());
            } else {
                logger.error("Error sending purchase", exception);
            }
        });
    }
    
    /**
     * Stops the producer and releases resources.
     */
    public void stop() {
        logger.info("Stopping purchase producer");
        scheduler.shutdown();
        producer.close();
    }
}
