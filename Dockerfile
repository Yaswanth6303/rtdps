# Build stage
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /app
COPY gradlew .
COPY gradle gradle
COPY build.gradle .
COPY settings.gradle .
# Grant execute permission to the gradle wrapper
RUN chmod +x gradlew
# Download dependencies
RUN ./gradlew dependencies --no-daemon

COPY src src
# Build the application (skipping tests during image build)
RUN ./gradlew build -x test --no-daemon

# Run stage
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Add a non-root user for security
RUN groupadd -r spring && useradd -r -g spring spring
USER spring:spring

# Copy the built jar from the build stage
COPY --from=build /app/build/libs/*.jar app.jar

EXPOSE 9191

ENTRYPOINT ["java", "-jar", "app.jar"]
