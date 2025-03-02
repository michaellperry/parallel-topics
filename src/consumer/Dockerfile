FROM maven:3.8.6-openjdk-11-slim AS build

WORKDIR /app

# Copy the parent POM
COPY pom.xml /app/

# Copy all modules
COPY src/common/pom.xml /app/src/common/
COPY src/common/src /app/src/common/src/

COPY src/producer/pom.xml /app/src/producer/
COPY src/producer/src /app/src/producer/src/

COPY src/consumer/pom.xml /app/src/consumer/
COPY src/consumer/src /app/src/consumer/src/

# Build the project
RUN mvn -f /app/pom.xml clean package

FROM openjdk:11-jre-slim

WORKDIR /app
COPY --from=build /app/src/consumer/target/consumer-1.0-SNAPSHOT-jar-with-dependencies.jar ./consumer.jar

CMD ["java", "-jar", "consumer.jar"]
