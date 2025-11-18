# --- Stage 1: Build ---
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app

# Copy Gradle wrapper and config first (Caching layer)
COPY gradlew .
COPY gradle gradle
COPY build.gradle.kts .
COPY settings.gradle.kts .

# Grant execution rights
RUN chmod +x ./gradlew

# Download dependencies without building (optimizes re-builds)
# Note: "clean" isn't needed here, just grab deps
RUN ./gradlew dependencies --no-daemon

# Now copy the source code
COPY src src

# Build the application (skip tests to save deployment minutes)
RUN ./gradlew bootJar --no-daemon -x test

# --- Stage 2: Run ---
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Create a non-root user (Security Best Practice)
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

# Copy jar from build stage
COPY --from=build /app/build/libs/*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]