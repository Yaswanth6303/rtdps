package me.yswnthdev.rtdps.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.dto.JobEvent;
import me.yswnthdev.rtdps.entity.JobTracker;
import me.yswnthdev.rtdps.kafka.JobEventProducer;
import me.yswnthdev.rtdps.repository.JobTrackerRepository;
import me.yswnthdev.rtdps.utils.BatchErrorLogger;
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

        long totalRead = 0, totalWritten = 0, totalSkipped = 0;
        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            totalRead    += stepExecution.getReadCount();
            totalWritten += stepExecution.getWriteCount();
            totalSkipped += stepExecution.getSkipCount();
        }

        long durationMs = 0;
        if (jobExecution.getStartTime() != null && jobExecution.getEndTime() != null) {
            durationMs = Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis();
        }

        if (status == BatchStatus.COMPLETED) {
            log.info("[JobCompletionListener] Job '{}' COMPLETED in {}ms — Read: {}, Written: {}, Skipped: {}",
                    jobName, durationMs, totalRead, totalWritten, totalSkipped);
        } else {
            log.error("[JobCompletionListener] Job '{}' FAILED with status: {} — Read: {}, Written: {}, Skipped: {}",
                    jobName, status, totalRead, totalWritten, totalSkipped);
        }

        String trackerId = jobExecution.getJobParameters().getString("trackerId");
        if (trackerId == null) return;

        // Flush + close the per-run skip log before we mark the tracker terminal.
        BatchErrorLogger.closeRun(trackerId);

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
        }

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
