package me.yswnthdev.rtdps.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.entity.JobTracker;
import me.yswnthdev.rtdps.repository.JobTrackerRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

/**
 * Consumes {@code job-requests.DLT} — the topic where poison messages land
 * after the main consumer exhausts its retry budget. Marks the originating
 * {@link JobTracker} as FAILED so the polling client sees a definitive status
 * instead of a stuck PROCESSING row, and logs the failure with the origin
 * coordinates for debugging.
 * <p>
 * Payloads are deserialized as {@code byte[]} because the message that
 * tripped the DLT may itself be unparseable JSON.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class JobRequestDltConsumer {

    private final JobTrackerRepository jobTrackerRepository;

    @KafkaListener(
            topics = "job-requests.DLT",
            groupId = "rtdps-dlt-group",
            containerFactory = "dltKafkaListenerContainerFactory"
    )
    public void consumeDeadLetter(ConsumerRecord<String, byte[]> record) {
        String trackerId      = record.key();
        String exceptionClass = headerString(record, KafkaHeaders.DLT_EXCEPTION_FQCN);
        String exceptionMsg   = headerString(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        String originalTopic  = headerString(record, KafkaHeaders.DLT_ORIGINAL_TOPIC);

        log.error("[JobRequestDltConsumer] Poison message — tracker={}, origin={}, ex={}: {}",
                trackerId, originalTopic, exceptionClass, exceptionMsg);

        if (trackerId != null) {
            jobTrackerRepository.findById(trackerId)
                    .ifPresent(t -> markFailed(t, exceptionClass, exceptionMsg));
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

    private static String headerString(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return (h == null || h.value() == null) ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
