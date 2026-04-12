package me.yswnthdev.rtdps.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.dto.JobEvent;
import me.yswnthdev.rtdps.entity.JobTracker;
import me.yswnthdev.rtdps.kafka.JobEventProducer;
import me.yswnthdev.rtdps.repository.JobTrackerRepository;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobCompletionListener implements JobExecutionListener {

    private final JobTrackerRepository jobTrackerRepository;
    private final JobEventProducer jobEventProducer;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("[JobCompletionListener] Job '{}' starting...",
                jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        BatchStatus status = jobExecution.getStatus();

        long totalRead = 0;
        long totalWritten = 0;
        long totalSkipped = 0;

        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            totalRead += stepExecution.getReadCount();
            totalWritten += stepExecution.getWriteCount();
            totalSkipped += stepExecution.getSkipCount();
        }

        long durationMs = 0;
        if (jobExecution.getStartTime() != null && jobExecution.getEndTime() != null) {
            durationMs = Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis();
        }

        // Log the result
        if (status == BatchStatus.COMPLETED) {
            log.info("[JobCompletionListener] Job '{}' COMPLETED in {}ms — Read: {}, Written: {}, Skipped: {}",
                    jobName, durationMs, totalRead, totalWritten, totalSkipped);
        } else {
            log.error("[JobCompletionListener] Job '{}' FAILED with status: {} — Read: {}, Written: {}, Skipped: {}",
                    jobName, status, totalRead, totalWritten, totalSkipped);
        }

        // Update job tracker if trackerId is present
        String trackerId = jobExecution.getJobParameters().getString("trackerId");
        if (trackerId != null) {
            JobTracker tracker = jobTrackerRepository.findById(trackerId).orElse(null);
            if (tracker != null) {
                tracker.setExecutionId(jobExecution.getId());
                tracker.setStatus(status.toString());
                tracker.setReadCount(totalRead);
                tracker.setWriteCount(totalWritten);
                tracker.setSkipCount(totalSkipped);
                tracker.setDurationMs(durationMs);
                tracker.setUpdatedAt(LocalDateTime.now());
                if (status != BatchStatus.COMPLETED) {
                    tracker.setErrorMessage(jobExecution.getExitStatus().getExitDescription());
                }
                jobTrackerRepository.save(tracker);

                log.info("[JobCompletionListener] JobTracker updated — tracker: {}, status: {}",
                        trackerId, status);

                if ("IMPORT".equals(tracker.getJobType()) && tracker.getFilePath() != null) {
                    try {
                        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(tracker.getFilePath()));
                        log.info("[JobCompletionListener] Cleaned up imported file at: {}", tracker.getFilePath());
                    } catch (java.io.IOException e) {
                        log.error("[JobCompletionListener] Failed to clean up imported file at: {}", tracker.getFilePath(), e);
                    }
                }
            }

            // Publish Kafka event
            JobEvent event = JobEvent.builder()
                    .trackerId(trackerId)
                    .executionId(jobExecution.getId())
                    .jobName(jobName)
                    .status(status.toString())
                    .readCount(totalRead)
                    .writeCount(totalWritten)
                    .skipCount(totalSkipped)
                    .durationMs(durationMs)
                    .completedAt(LocalDateTime.now())
                    .build();
            jobEventProducer.publishJobEvent(event);
        }
    }
}
