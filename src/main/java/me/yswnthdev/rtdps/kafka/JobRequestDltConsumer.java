package me.yswnthdev.rtdps.kafka;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.alerting.AlertPublisher;
import me.yswnthdev.rtdps.alerting.DltAlert;
import me.yswnthdev.rtdps.entity.JobTracker;
import me.yswnthdev.rtdps.repository.JobTrackerRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Consumes {@code job-requests.DLT} — the topic where poison messages land
 * after the main consumer exhausts its retry budget. Three responsibilities:
 *
 * <ol>
 *   <li>Mark the originating {@code JobTracker} as FAILED so the polling
 *       client sees a definitive answer instead of a stuck PROCESSING row.</li>
 *   <li>Fan out a structured {@link DltAlert} to every {@link AlertPublisher}
 *       bean — at minimum logging, optionally a webhook.</li>
 *   <li>Increment a {@link Counter} so the DLT rate is visible on
 *       {@code /actuator/prometheus}.</li>
 * </ol>
 *
 * Payloads are deserialized as {@code byte[]} because the message that
 * tripped the DLT may itself be unparseable JSON.
 */
@Component
@Slf4j
public class JobRequestDltConsumer {

    private final JobTrackerRepository jobTrackerRepository;
    private final List<AlertPublisher> alertPublishers;
    private final Counter dltCounter;

    public JobRequestDltConsumer(JobTrackerRepository jobTrackerRepository,
                                 List<AlertPublisher> alertPublishers,
                                 MeterRegistry meterRegistry) {
        this.jobTrackerRepository = jobTrackerRepository;
        this.alertPublishers = alertPublishers;
        this.dltCounter = Counter.builder("rtdps.dlt.messages.total")
                .description("Messages routed to job-requests.DLT after exhausting retries")
                .tag("topic", "job-requests.DLT")
                .register(meterRegistry);
    }

    @KafkaListener(
            topics = "job-requests.DLT",
            groupId = "rtdps-dlt-group",
            containerFactory = "dltKafkaListenerContainerFactory"
    )
    public void consumeDeadLetter(ConsumerRecord<String, byte[]> record) {
        String trackerId       = record.key();
        String exceptionClass  = headerString(record, KafkaHeaders.DLT_EXCEPTION_FQCN);
        String exceptionMsg    = headerString(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        String originalTopic   = headerString(record, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        Integer originalPart   = headerInt(record,    KafkaHeaders.DLT_ORIGINAL_PARTITION);
        Long originalOffset    = headerLong(record,   KafkaHeaders.DLT_ORIGINAL_OFFSET);
        String payloadPreview  = preview(record.value());

        dltCounter.increment();

        log.error("[JobRequestDltConsumer] Poison message — tracker={}, origin={}-{}@{}, ex={}: {}",
                trackerId, originalTopic, originalPart, originalOffset, exceptionClass, exceptionMsg);

        if (trackerId != null) {
            jobTrackerRepository.findById(trackerId).ifPresent(t -> markFailed(t, exceptionClass, exceptionMsg));
        }

        DltAlert alert = new DltAlert(
                trackerId,
                originalTopic,
                originalPart,
                originalOffset,
                exceptionClass,
                exceptionMsg,
                payloadPreview,
                Instant.now()
        );
        for (AlertPublisher publisher : alertPublishers) {
            try {
                publisher.publish(alert);
            } catch (Exception e) {
                log.warn("[JobRequestDltConsumer] AlertPublisher {} threw: {}",
                        publisher.getClass().getSimpleName(), e.getMessage());
            }
        }
    }

    private void markFailed(JobTracker tracker, String exceptionClass, String exceptionMsg) {
        // Don't overwrite a job that already completed — DLT delivery might be replayed.
        if ("COMPLETED".equals(tracker.getStatus())) return;
        tracker.setStatus("FAILED");
        tracker.setErrorMessage("Sent to DLT — " + exceptionClass + ": " + exceptionMsg);
        tracker.setUpdatedAt(LocalDateTime.now());
        jobTrackerRepository.save(tracker);
    }

    // ----------------- header helpers -----------------

    private static String headerString(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return (h == null || h.value() == null) ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static Integer headerInt(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        if (h == null || h.value() == null || h.value().length < 4) return null;
        // DeadLetterPublishingRecoverer encodes partition as 4-byte big-endian int.
        byte[] v = h.value();
        return ((v[0] & 0xFF) << 24) | ((v[1] & 0xFF) << 16) | ((v[2] & 0xFF) << 8) | (v[3] & 0xFF);
    }

    private static Long headerLong(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        if (h == null || h.value() == null || h.value().length < 8) return null;
        // Offset is 8-byte big-endian long.
        byte[] v = h.value();
        long result = 0L;
        for (int i = 0; i < 8; i++) result = (result << 8) | (v[i] & 0xFF);
        return result;
    }

    private static String preview(byte[] value) {
        if (value == null) return "";
        int len = Math.min(value.length, 512);
        return new String(value, 0, len, StandardCharsets.UTF_8).replace('\n', ' ').replace('\r', ' ');
    }
}
