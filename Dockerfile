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

# Non-root user + writable /app so JobController can create /app/uploads at runtime
# (application.yml resolves app.upload-dir to ${user.dir}/uploads = /app/uploads).
RUN groupadd -r spring \
 && useradd -r -g spring spring \
 && mkdir -p /app/uploads \
 && chown -R spring:spring /app
USER spring:spring

# Copy the built jar with correct ownership so the spring user can read it.
COPY --from=build --chown=spring:spring /app/build/libs/*.jar app.jar

EXPOSE 9191

ENTRYPOINT ["java", "-jar", "app.jar"]
