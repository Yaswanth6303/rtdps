package me.yswnthdev.rtdps.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.dto.JobRequest;
import me.yswnthdev.rtdps.entity.JobTracker;
import me.yswnthdev.rtdps.repository.JobTrackerRepository;
import me.yswnthdev.rtdps.service.BatchJobService;
import me.yswnthdev.rtdps.storage.StorageBackend;
import org.springframework.batch.core.JobExecution;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobRequestConsumer {

    private final BatchJobService batchJobService;
    private final JobTrackerRepository jobTrackerRepository;
    private final StorageBackend storage;

    @KafkaListener(topics = "job-requests", groupId = "rtdps-group")
    public void consumeJobRequest(JobRequest request) {
        log.info("[JobRequestConsumer] Received job request — tracker: {}, type: {}",
                request.getTrackerId(), request.getJobType());

        JobTracker tracker = jobTrackerRepository.findById(request.getTrackerId()).orElse(null);
        if (tracker == null) {
            log.error("[JobRequestConsumer] Tracker not found: {}", request.getTrackerId());
            return;
        }

        tracker.setStatus("PROCESSING");
        tracker.setUpdatedAt(LocalDateTime.now());
        jobTrackerRepository.save(tracker);

        try {
            JobExecution execution;

            if ("IMPORT".equals(request.getJobType())) {
                // Pull durable copy down to a local path the batch reader can stream.
                Path localInput = storage.materialize(request.getFilePath());
                execution = batchJobService.launchImportJob(
                        localInput.toString(),         // batch reads here
                        request.getFilePath(),         // durable origin — listener deletes after success
                        request.getTrackerId(),
                        request.getConflictPolicy()); // SKIP / UPDATE
            } else {
                // Export: allocate a local temp path for the writer; listener uploads to durable storage.
                Path localOutput = storage.allocateLocalExportPath(request.getTrackerId());
                String hint = "customers_" + System.currentTimeMillis() + ".csv";

                execution = batchJobService.launchExportJob(
                        localOutput.toString(),    // batch writes here
                        hint,                       // filename hint for finalizeFromLocal
                        request.getTrackerId());
            }

            log.info("[JobRequestConsumer] Job launched — tracker: {}, executionId: {}, status: {}",
                    request.getTrackerId(), execution.getId(), execution.getStatus());

        } catch (Exception e) {
            log.error("[JobRequestConsumer] Failed to process job request — tracker: {}",
                    request.getTrackerId(), e);

            tracker.setStatus("FAILED");
            tracker.setErrorMessage(e.getMessage());
            tracker.setUpdatedAt(LocalDateTime.now());
            jobTrackerRepository.save(tracker);
        }
    }
}
