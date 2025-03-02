# Kafka Streams Example

This project demonstrates how to use Kafka Streams to process product purchase data. It consists of two main components:

1. **Producer**: Generates a new purchase message every second and publishes it to a Kafka topic.
2. **Processor**: Uses Kafka Streams to process the purchase data, remap the key from order ID to SKU, and aggregate quantities by SKU.

## Project Structure

```
kafka-streams-example/
├── mesh/
│   └── docker-compose.yml       # Container definitions
└── src/
    ├── common/                  # Shared models and utilities
    │   ├── Dockerfile
    │   ├── pom.xml
    │   └── src/main/java/...    # Purchase model classes
    ├── producer/                # Generates purchase messages
    │   ├── Dockerfile
    │   ├── pom.xml
    │   └── src/main/java/...    # Producer application code
    └── consumer/               # Kafka Streams application
        ├── Dockerfile
        ├── pom.xml
        └── src/main/java/...    # Streams processing code
```

## Data Model

The `Purchase` class represents a product purchase with the following fields:
- `orderId`: Unique identifier for the order (used as the message key by the producer)
- `customerId`: Identifier for the customer
- `sku`: Stock Keeping Unit - identifier for the product
- `quantity`: Number of items purchased
- `unitPrice`: Price per item
- `extendedPrice`: Total price (quantity × unitPrice)

## Kafka Streams Topology

The consumer implements the following Kafka Streams topology:

1. Consume purchase messages from the input topic (key = orderId)
2. Remap the messages to use SKU as the key instead of orderId
3. Group the messages by SKU
4. Aggregate the quantities for each SKU
5. Output the results to a KTable and a topic

## Running the Example

### Prerequisites

- Docker and Docker Compose

### Steps

1. Start the containers:
   ```
   cd mesh
   docker compose pull
   docker compose up -d --build
   ```

2. Check the logs to see the producer generating purchase data:
   ```
   docker logs -f purchase-producer
   ```

3. Check the logs to see the consumer processing the data:
   ```
   docker logs -f purchase-consumer
   ```

4. To stop the example when you are finished:
   ```
   docker compose down -v
   ```

## Inspecting the KTable and Topics

### Using the REST Endpoint

The consumer exposes a REST endpoint to query the state of the KTable. You can access it at:

```
http://localhost:8080/sku-totals
```

This will return a JSON object with the aggregated quantities by SKU.

To see the status of the application startup, you can check the status endpoint at:

```
http://localhost:8080/status
```

You can check the health of the application at:

```
http://localhost:8080/health
```

### Using Kafka CLI

1. Attach to the Kafka CLI container:
   ```
   docker exec -it kafka-cli /bin/bash
   ```

2. List all Kafka topics:
   ```
   kafka-topics --bootstrap-server kafka:9092 --list
   ```

3. Describe a specific topic:
   ```
   kafka-topics --bootstrap-server kafka:9092 --describe --topic <topic-name>
   ```

4. Consume messages from a topic:
   ```
   kafka-console-consumer --bootstrap-server kafka:9092 --topic sku-totals --property print.key=true
   ```

By following these steps, you can inspect the KTable and topics used in your Kafka Streams example.

## Environment Variables

### Producer
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default: "localhost:9092")
- `TOPIC_NAME`: Topic to publish purchase messages to (default: "purchases")
- `PUBLISH_INTERVAL_MS`: Interval between generated purchases in milliseconds (default: 1000)

### Processor
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default: "localhost:9092")
- `APPLICATION_ID`: Kafka Streams application ID (default: "purchase-consumer")
- `INPUT_TOPIC`: Topic to consume purchase messages from (default: "purchases")
- `OUTPUT_TOPIC`: Topic to output aggregated results to (default: "sku-totals")
