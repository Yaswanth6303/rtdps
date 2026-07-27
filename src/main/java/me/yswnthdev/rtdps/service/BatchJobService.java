package me.yswnthdev.rtdps.service;

import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.utils.BatchErrorLogger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class BatchJobService {

    private final JobLauncher jobLauncher;
    private final Job importJob;
    private final Job exportJob;

    public BatchJobService(JobLauncher jobLauncher,
                           @Qualifier("importJob") Job importJob,
                           @Qualifier("exportJob") Job exportJob) {
        this.jobLauncher = jobLauncher;
        this.importJob = importJob;
        this.exportJob = exportJob;
    }

    /**
     * @param localFilePath      path the batch reader streams from (may be a temp materialization)
     * @param originalLocation   durable storage location (s3://… or local path) the listener
     *                           should delete after the job succeeds
     * @param conflictPolicy     SKIP (default) or UPDATE — drives the writer's ON CONFLICT clause
     */
    public JobExecution launchImportJob(String localFilePath, String originalLocation,
                                        String trackerId, String conflictPolicy) {
        String resolvedPolicy = (conflictPolicy == null || conflictPolicy.isBlank()) ? "SKIP" : conflictPolicy;
        log.info("[BatchJobService] Launching import job — local: {}, origin: {}, tracker: {}, policy: {}",
                localFilePath, originalLocation, trackerId, resolvedPolicy);

        BatchErrorLogger.initRun(trackerId);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("filePath", localFilePath)
                .addString("originalLocation", originalLocation)
                .addString("trackerId", trackerId)
                .addString("conflictPolicy", resolvedPolicy)
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();

        try {
            return jobLauncher.run(importJob, jobParameters);
        } catch (Exception e) {
            log.error("[BatchJobService] Import job failed to launch", e);
            throw new RuntimeException("Failed to launch import job", e);
        }
    }

    /**
     * @param localOutputPath   temp path the writer fills with the CSV
     * @param filenameHint      name the listener uses when uploading to durable storage
     */
    public JobExecution launchExportJob(String localOutputPath, String filenameHint, String trackerId) {
        log.info("[BatchJobService] Launching export job — local: {}, hint: {}, tracker: {}",
                localOutputPath, filenameHint, trackerId);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("outputPath", localOutputPath)
                .addString("exportFilenameHint", filenameHint)
                .addString("trackerId", trackerId)
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();

        try {
            return jobLauncher.run(exportJob, jobParameters);
        } catch (Exception e) {
            log.error("[BatchJobService] Export job failed to launch", e);
            throw new RuntimeException("Failed to launch export job", e);
        }
    }
}
