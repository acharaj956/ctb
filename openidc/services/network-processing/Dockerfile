# Build context is the repo root (see docker-compose.yml).
# Stage 1: build the fat jar with Maven.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY services/network-processing/pom.xml .
COPY services/network-processing/src ./src
RUN mvn -q -B -DskipTests package

# Stage 2: slim runtime image with just the JRE + jar.
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/network-processing.jar app.jar
CMD ["java", "-jar", "app.jar"]
