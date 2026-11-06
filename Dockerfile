# --- Stage 0: Runtime JRE Base ---
FROM eclipse-temurin:21-jre-alpine AS jre-base

# Probe actual runtime JRE modules via java --list-modules (do not trust release MODULES)
FROM jre-base AS jre-probe
RUN /opt/java/openjdk/bin/java --version > /tmp/jre-java-version.txt && \
    /opt/java/openjdk/bin/java --list-modules | sed 's/@.*//' | sort > /tmp/jre-list-modules.txt

# --- Stage 1: Build ---
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app

# Verify exact JDK/JRE version parity and link ONLY jdk.jcmd (+ jdk.attach, jdk.internal.jvmstat)
# onto the actual runtime JRE module set from java --list-modules.
COPY --from=jre-probe /tmp/jre-java-version.txt /tmp/jre-java-version.txt
COPY --from=jre-probe /tmp/jre-list-modules.txt /tmp/jre-list-modules.txt
RUN /opt/java/openjdk/bin/java --version > /tmp/jdk-java-version.txt && \
    diff -u /tmp/jre-java-version.txt /tmp/jdk-java-version.txt && \
    JRE_MODS=$(paste -sd, /tmp/jre-list-modules.txt) && \
    /opt/java/openjdk/bin/jlink \
      --add-modules "${JRE_MODS},jdk.jcmd" \
      --strip-debug \
      --no-man-pages \
      --no-header-files \
      --compress=zip-6 \
      --output /opt/jre-with-jcmd && \
    /opt/jre-with-jcmd/bin/java --list-modules | sed 's/@.*//' | sort > /tmp/new-list-modules.txt && \
    REMOVED_MODS=$(comm -23 /tmp/jre-list-modules.txt /tmp/new-list-modules.txt | paste -sd, -) && \
    ADDED_MODS=$(comm -13 /tmp/jre-list-modules.txt /tmp/new-list-modules.txt | paste -sd, -) && \
    echo "Removed modules vs runtime JRE: '${REMOVED_MODS}'" && \
    echo "Added modules vs runtime JRE: '${ADDED_MODS}'" && \
    test -z "${REMOVED_MODS}" && \
    test "${ADDED_MODS}" = "jdk.attach,jdk.internal.jvmstat,jdk.jcmd"

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

# Regenerate default CDS archive (classes.jsa) for the updated lib/modules and create non-root user
RUN /opt/java/openjdk/bin/java -Xshare:dump && \
    addgroup -S spring && adduser -S spring -G spring
USER spring:spring

# Copy jar from build stage
COPY --from=build /app/build/libs/*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-XX:TieredStopAtLevel=1", "-XX:+UseSerialGC", "-Xms32m", "-XX:MaxRAMPercentage=40.0", "-XX:MinHeapFreeRatio=10", "-XX:MaxHeapFreeRatio=25", "-XX:-ShrinkHeapInSteps", "-XX:MaxMetaspaceSize=128m", "-XX:ReservedCodeCacheSize=32m", "-XX:CICompilerCount=2", "-XX:MaxDirectMemorySize=32m", "-Dio.netty.noPreferDirect=true", "-Dio.netty.maxDirectMemory=0", "-Djdk.nio.maxCachedBufferSize=32768", "-Xss384k", "-XX:FlightRecorderOptions=stackdepth=32,memorysize=2m", "-jar", "app.jar"]