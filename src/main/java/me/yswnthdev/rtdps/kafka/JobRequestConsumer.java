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

import java.time.LocalDateTime;

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

        JobTracker tracker = jobTrackerRepository.findById(request.getTrackerId()).orElse(null);
        if (tracker == null) {
            log.error("[JobRequestConsumer] Tracker not found: {}", request.getTrackerId());
            return;
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
