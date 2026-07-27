package me.yswnthdev.rtdps.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.dto.JobEvent;
import me.yswnthdev.rtdps.entity.JobTracker;
import me.yswnthdev.rtdps.kafka.JobEventProducer;
import me.yswnthdev.rtdps.repository.JobTrackerRepository;
import me.yswnthdev.rtdps.service.ConflictAwareCustomerWriter;
import me.yswnthdev.rtdps.storage.StorageBackend;
import me.yswnthdev.rtdps.utils.BatchErrorLogger;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class JobCompletionListener implements JobExecutionListener {

    private final JobTrackerRepository jobTrackerRepository;
    private final JobEventProducer jobEventProducer;
    private final StorageBackend storage;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("[JobCompletionListener] Job '{}' starting...",
                jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        BatchStatus status = jobExecution.getStatus();

        long totalRead = 0, totalWritten = 0, totalSkipped = 0, totalFiltered = 0;
        long totalInserted = 0, totalUpdated = 0, totalConflicted = 0;
        for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
            totalRead     += stepExecution.getReadCount();
            totalWritten  += stepExecution.getWriteCount();
            totalSkipped  += stepExecution.getSkipCount();
            totalFiltered += stepExecution.getFilterCount();
            ExecutionContext ec = stepExecution.getExecutionContext();
            totalInserted   += ec.getLong(ConflictAwareCustomerWriter.CTX_INSERTED,  0L);
            totalUpdated    += ec.getLong(ConflictAwareCustomerWriter.CTX_UPDATED,   0L);
            totalConflicted += ec.getLong(ConflictAwareCustomerWriter.CTX_CONFLICTS, 0L);
        }

        long durationMs = 0;
        if (jobExecution.getStartTime() != null && jobExecution.getEndTime() != null) {
            durationMs = Duration.between(jobExecution.getStartTime(), jobExecution.getEndTime()).toMillis();
        }

        if (status == BatchStatus.COMPLETED) {
            log.info("[JobCompletionListener] Job '{}' COMPLETED in {}ms — Read: {}, Written: {}, Skipped: {}, Filtered: {}",
                    jobName, durationMs, totalRead, totalWritten, totalSkipped, totalFiltered);
        } else {
            log.error("[JobCompletionListener] Job '{}' FAILED with status: {} — Read: {}, Written: {}, Skipped: {}, Filtered: {}",
                    jobName, status, totalRead, totalWritten, totalSkipped, totalFiltered);
        }

        String trackerId = jobExecution.getJobParameters().getString("trackerId");
        if (trackerId == null) return;

        JobTracker tracker = jobTrackerRepository.findById(trackerId).orElse(null);
        if (tracker != null) {
            tracker.setExecutionId(jobExecution.getId());
            tracker.setStatus(status.toString());
            tracker.setReadCount(totalRead);
            tracker.setWriteCount(totalWritten);
            tracker.setSkipCount(totalSkipped);
            tracker.setFilteredCount(totalFiltered);
            tracker.setInsertedCount(totalInserted);
            tracker.setUpdatedCount(totalUpdated);
            tracker.setConflictCount(totalConflicted);
            tracker.setDurationMs(durationMs);
            tracker.setUpdatedAt(LocalDateTime.now());
            if (status != BatchStatus.COMPLETED) {
                tracker.setErrorMessage(jobExecution.getExitStatus().getExitDescription());
            }

            if ("IMPORT".equals(tracker.getJobType())) {
                handleImportCleanup(jobExecution, status);
            } else if ("EXPORT".equals(tracker.getJobType()) && status == BatchStatus.COMPLETED) {
                handleExportFinalize(jobExecution, tracker);
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
                .filteredCount(totalFiltered)
                .insertedCount(totalInserted)
                .updatedCount(totalUpdated)
                .conflictCount(totalConflicted)
                .durationMs(durationMs)
                .completedAt(LocalDateTime.now())
                .build();
        jobEventProducer.publishJobEvent(event);
    }

    private void handleImportCleanup(JobExecution jobExecution, BatchStatus status) {
        String materialized = jobExecution.getJobParameters().getString("filePath");
        String original     = jobExecution.getJobParameters().getString("originalLocation");
        String trackerId    = jobExecution.getJobParameters().getString("trackerId");

        // Flush + close the per-run error log so the file is durable before the tracker is marked terminal.
        if (trackerId != null) {
            BatchErrorLogger.closeRun(trackerId);
        }

        // Drop the materialized local copy unconditionally.
        if (materialized != null) {
            try {
                storage.releaseLocal(Paths.get(materialized), original);
            } catch (IOException e) {
                log.warn("[JobCompletionListener] Failed to release materialized path {}: {}",
                        materialized, e.getMessage());
            }
        }

        // Only delete the durable origin on success — keep it on failure so the user can retry.
        if (status == BatchStatus.COMPLETED && original != null) {
            try {
                storage.delete(original);
                log.info("[JobCompletionListener] Cleaned up durable origin: {}", original);
            } catch (IOException e) {
                log.warn("[JobCompletionListener] Failed to delete durable origin {}: {}",
                        original, e.getMessage());
            }
        }
    }

    private void handleExportFinalize(JobExecution jobExecution, JobTracker tracker) {
        String localOutputPath = jobExecution.getJobParameters().getString("outputPath");
        String filenameHint    = jobExecution.getJobParameters().getString("exportFilenameHint");
        if (localOutputPath == null || filenameHint == null) {
            log.warn("[JobCompletionListener] Export missing outputPath/hint — cannot finalize");
            return;
        }
        Path local = Paths.get(localOutputPath);
        try {
            String durableLocation = storage.finalizeFromLocal(local, filenameHint);
            tracker.setFilePath(durableLocation);
            log.info("[JobCompletionListener] Export finalized to: {}", durableLocation);
        } catch (IOException e) {
            log.error("[JobCompletionListener] Failed to finalize export {}: {}", localOutputPath, e.getMessage());
            tracker.setStatus("FAILED");
            tracker.setErrorMessage("Failed to upload export: " + e.getMessage());
        }
    }
}
