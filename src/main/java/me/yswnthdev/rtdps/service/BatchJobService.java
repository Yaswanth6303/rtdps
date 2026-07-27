package me.yswnthdev.rtdps.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.utils.BatchErrorLogger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class BatchJobService {

    private final JobLauncher jobLauncher;
    private final Job importJob;

    public JobExecution launchImportJob(String filePath, String trackerId) {
        log.info("[BatchJobService] Launching import job — file: {}, tracker: {}", filePath, trackerId);

        BatchErrorLogger.initRun(trackerId);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("filePath", filePath)
                .addString("trackerId", trackerId)
                .addLong("startAt", System.currentTimeMillis()) // uniqueness for JobInstance
                .toJobParameters();

        try {
            return jobLauncher.run(importJob, jobParameters);
        } catch (Exception e) {
            log.error("[BatchJobService] Import job failed to launch", e);
            throw new RuntimeException("Failed to launch import job", e);
        }
    }
}
