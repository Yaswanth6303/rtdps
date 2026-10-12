package me.yswnthdev.rtdps.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.dto.JobRequest;
import me.yswnthdev.rtdps.entity.JobTracker;
import me.yswnthdev.rtdps.repository.JobTrackerRepository;
import me.yswnthdev.rtdps.service.BatchJobService;
import org.springframework.batch.core.JobExecution;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;

/**
 * Launches an import job per {@link JobRequest}. Failures are rethrown, never
 * swallowed, so the container's {@code DefaultErrorHandler} owns them:
 * <ul>
 *   <li>{@link IllegalArgumentException} (bad request, missing tracker or file)
 *       is non-retryable and goes straight to {@code job-requests.DLT}.</li>
 *   <li>Anything else (e.g. a transient launch failure) is retried 3× with
 *       backoff, then sent to the DLT.</li>
 * </ul>
 * {@code JobRequestDltConsumer} then marks the tracker FAILED.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class JobRequestConsumer {

    private final BatchJobService batchJobService;
    private final JobTrackerRepository jobTrackerRepository;

    @KafkaListener(topics = "job-requests", groupId = "rtdps-group")
    public void consumeJobRequest(JobRequest request) {
        log.info("[JobRequestConsumer] Received job request — tracker: {}, type: {}",
                request.getTrackerId(), request.getJobType());

        if (request.getTrackerId() == null || request.getFilePath() == null) {
            throw new IllegalArgumentException("Job request missing trackerId or filePath");
        }

        JobTracker tracker = jobTrackerRepository.findById(request.getTrackerId())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Tracker not found: " + request.getTrackerId()));

        // Kafka is at-least-once — a redelivered request for a job that already
        // started (or finished) must not launch a second run.
        if (!"QUEUED".equals(tracker.getStatus())) {
            log.warn("[JobRequestConsumer] Ignoring duplicate request — tracker: {} is already {}",
                    tracker.getId(), tracker.getStatus());
            return;
        }

        if (!Files.isReadable(Path.of(request.getFilePath()))) {
            throw new IllegalArgumentException("Upload file not readable: " + request.getFilePath());
        }

        tracker.setStatus("PROCESSING");
        tracker.setUpdatedAt(LocalDateTime.now());
        jobTrackerRepository.save(tracker);

        try {
            JobExecution execution = batchJobService.launchImportJob(
                    request.getFilePath(),
                    request.getTrackerId());

            log.info("[JobRequestConsumer] Job launched — tracker: {}, executionId: {}, status: {}",
                    request.getTrackerId(), execution.getId(), execution.getStatus());

        } catch (RuntimeException e) {
            log.error("[JobRequestConsumer] Failed to launch job — tracker: {}", request.getTrackerId(), e);

            // Back to QUEUED so the error handler's retry passes the duplicate guard above.
            tracker.setStatus("QUEUED");
            tracker.setUpdatedAt(LocalDateTime.now());
            jobTrackerRepository.save(tracker);
            throw e;
        }
    }
}
