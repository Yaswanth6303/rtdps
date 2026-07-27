package me.yswnthdev.rtdps.alerting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Optional publisher that POSTs the alert as JSON to a webhook
 * (Slack incoming hook, PagerDuty events API, custom endpoint).
 * <p>
 * Active only when {@code app.alerts.webhook-url} is set.
 * Calls are fire-and-forget on a small background pool so the Kafka consumer
 * thread is never blocked by a slow webhook.
 */
@Component
@Slf4j
@ConditionalOnProperty(name = "app.alerts.webhook-url")
public class WebhookAlertPublisher implements AlertPublisher {

    private final URI webhookUrl;
    private final ObjectMapper mapper;
    private final HttpClient http;
    private final ExecutorService dispatcher = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "dlt-webhook-dispatcher");
        t.setDaemon(true);
        return t;
    });

    public WebhookAlertPublisher(@Value("${app.alerts.webhook-url}") String webhookUrl,
                                 ObjectMapper mapper) {
        this.webhookUrl = URI.create(webhookUrl);
        this.mapper = mapper;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        log.info("[WebhookAlertPublisher] DLT alerts will be POSTed to {}", webhookUrl);
    }

    @Override
    public void publish(DltAlert alert) {
        dispatcher.submit(() -> send(alert));
    }

    private void send(DltAlert alert) {
        String body;
        try {
            body = mapper.writeValueAsString(alert);
        } catch (JsonProcessingException e) {
            log.warn("[WebhookAlertPublisher] Failed to serialize alert: {}", e.getMessage());
            return;
        }
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(webhookUrl)
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() >= 300) {
                log.warn("[WebhookAlertPublisher] Webhook returned {} for tracker {}: {}",
                        res.statusCode(), alert.trackerId(), res.body());
            }
        } catch (Exception e) {
            log.warn("[WebhookAlertPublisher] Webhook delivery failed for tracker {}: {}",
                    alert.trackerId(), e.getMessage());
        }
    }

    @PreDestroy
    void shutdown() {
        dispatcher.shutdown();
    }
}
