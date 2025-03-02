FROM maven:3.8.6-openjdk-11-slim AS build

WORKDIR /app

# Copy the parent POM
COPY pom.xml /app/

# Copy all modules
COPY src/common/pom.xml /app/src/common/
COPY src/common/src /app/src/common/src/

COPY src/producer/pom.xml /app/src/producer/
COPY src/producer/src /app/src/producer/src/

COPY src/processor/pom.xml /app/src/processor/
COPY src/processor/src /app/src/processor/src/

# Build the project
RUN mvn -f /app/pom.xml clean package

FROM openjdk:11-jre-slim

WORKDIR /app
COPY --from=build /app/src/producer/target/producer-1.0-SNAPSHOT-jar-with-dependencies.jar ./producer.jar

CMD ["java", "-jar", "producer.jar"]
