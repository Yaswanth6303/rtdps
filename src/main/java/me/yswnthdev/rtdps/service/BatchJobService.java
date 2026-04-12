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

    public JobExecution launchImportJob(String filePath, String trackerId) {
        log.info("[BatchJobService] Launching import job — file: {}, tracker: {}", filePath, trackerId);

        BatchErrorLogger.initRun(trackerId);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("filePath", filePath)
                .addString("trackerId", trackerId)
                .addLong("startAt", System.currentTimeMillis())
                .toJobParameters();

        try {
            return jobLauncher.run(importJob, jobParameters);
        } catch (Exception e) {
            log.error("[BatchJobService] Import job failed to launch", e);
            throw new RuntimeException("Failed to launch import job", e);
        }
    }

    public JobExecution launchExportJob(String outputPath, String trackerId) {
        log.info("[BatchJobService] Launching export job — output: {}, tracker: {}", outputPath, trackerId);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("outputPath", outputPath)
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
