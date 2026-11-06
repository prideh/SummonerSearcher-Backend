# --- Stage 0: Runtime JRE Base ---
FROM eclipse-temurin:21-jre-alpine AS jre-base

# --- Stage 1: Build ---
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app

# Verify exact JDK/JRE version parity and link ONLY jdk.jcmd (+ jdk.attach, jdk.internal.jvmstat)
# onto the existing JRE module set so runtime stays JRE-sized without full JDK packages.
COPY --from=jre-base /opt/java/openjdk/release /tmp/jre-release
RUN grep '^FULL_VERSION=' /tmp/jre-release && \
    grep '^FULL_VERSION=' /opt/java/openjdk/release && \
    test "$(grep '^FULL_VERSION=' /tmp/jre-release)" = "$(grep '^FULL_VERSION=' /opt/java/openjdk/release)" && \
    JRE_MODS=$(grep '^MODULES=' /tmp/jre-release | cut -d'"' -f2 | tr ' ' ',') && \
    /opt/java/openjdk/bin/jlink \
      --add-modules "${JRE_MODS},jdk.jcmd" \
      --strip-debug \
      --no-man-pages \
      --no-header-files \
      --compress=zip-6 \
      --output /opt/jre-with-jcmd

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
FROM jre-base
WORKDIR /app

# Copy only jcmd launcher, libattach.so, and the JRE+jdk.jcmd module image from the verified build stage
COPY --from=build /opt/jre-with-jcmd/bin/jcmd /opt/java/openjdk/bin/jcmd
COPY --from=build /opt/jre-with-jcmd/lib/libattach.so /opt/java/openjdk/lib/libattach.so
COPY --from=build /opt/jre-with-jcmd/lib/modules /opt/java/openjdk/lib/modules
COPY --from=build /opt/jre-with-jcmd/release /opt/java/openjdk/release

# Create a non-root user (Security Best Practice)
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

# Copy jar from build stage
COPY --from=build /app/build/libs/*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-XX:TieredStopAtLevel=1", "-XX:+UseSerialGC", "-Xms32m", "-XX:MaxRAMPercentage=40.0", "-XX:MinHeapFreeRatio=10", "-XX:MaxHeapFreeRatio=25", "-XX:-ShrinkHeapInSteps", "-XX:MaxMetaspaceSize=128m", "-XX:ReservedCodeCacheSize=32m", "-XX:CICompilerCount=2", "-XX:MaxDirectMemorySize=32m", "-Dio.netty.noPreferDirect=true", "-Dio.netty.maxDirectMemory=0", "-Djdk.nio.maxCachedBufferSize=32768", "-Xss384k", "-XX:FlightRecorderOptions=stackdepth=32,memorysize=2m", "-jar", "app.jar"]