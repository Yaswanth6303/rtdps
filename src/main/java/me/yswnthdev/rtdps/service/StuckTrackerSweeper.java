package me.yswnthdev.rtdps.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.entity.JobTracker;
import me.yswnthdev.rtdps.repository.JobTrackerRepository;
import me.yswnthdev.rtdps.storage.StorageBackend;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Periodic reconciliation task that finds {@code JobTracker} rows stuck in
 * {@code QUEUED} or {@code PROCESSING} and corrects them. Two classes of bugs
 * collapse into one fix:
 *
 * <ul>
 *   <li><b>Stuck PROCESSING (#6):</b> the consumer flips the tracker to
 *       PROCESSING <em>before</em> launching the job. A {@code kill -9} between
 *       those two operations leaves the tracker pinned forever.</li>
 *   <li><b>Listener-crash window (#3):</b> {@code JobCompletionListener}
 *       runs cleanup before persisting the tracker. A JVM crash between
 *       the two leaves the tracker stuck PROCESSING even though Spring Batch
 *       knows the job actually finished.</li>
 * </ul>
 *
 * <p>For each candidate the sweeper consults {@link JobExplorer}:
 * <ul>
 *   <li><b>JobExecution still running</b> — leave alone (legitimately long job).</li>
 *   <li><b>JobExecution terminal</b> — copy its status onto the tracker
 *       (COMPLETED / FAILED / STOPPED). For successful IMPORT jobs, replay
 *       storage cleanup defensively — {@code storage.delete} is idempotent.</li>
 *   <li><b>No JobExecution found</b> — mark the tracker {@code ABANDONED}.
 *       Operator can re-submit if the upload was important; the durable file
 *       is preserved so the original payload is recoverable.</li>
 * </ul>
 *
 * <p>Two metrics get exposed at {@code /actuator/prometheus}:
 * {@code rtdps.sweeper.runs.total} and {@code rtdps.sweeper.reconciled.total}.
 *
 * <p>Multiple application instances will each run their own sweeper —
 * reconciliation is idempotent (everyone wants the same final state) so no
 * leader election is strictly required. If reconciliation churn becomes
 * visible at scale, wire in something like ShedLock here.
 */
@Component
@Slf4j
@ConditionalOnProperty(name = "app.sweeper.enabled", havingValue = "true", matchIfMissing = true)
public class StuckTrackerSweeper {

    private final JobTrackerRepository repo;
    private final JobExplorer jobExplorer;
    private final StorageBackend storage;
    private final Counter reconciledCounter;
    private final Counter sweepRunCounter;

    @Value("${app.sweeper.stuck-threshold-minutes:15}")
    private long stuckThresholdMinutes;

    public StuckTrackerSweeper(JobTrackerRepository repo,
                               JobExplorer jobExplorer,
                               StorageBackend storage,
                               MeterRegistry meterRegistry) {
        this.repo = repo;
        this.jobExplorer = jobExplorer;
        this.storage = storage;
        this.reconciledCounter = Counter.builder("rtdps.sweeper.reconciled.total")
                .description("Stuck trackers reconciled by the sweeper")
                .register(meterRegistry);
        this.sweepRunCounter = Counter.builder("rtdps.sweeper.runs.total")
                .description("Total sweep runs (including no-op runs)")
                .register(meterRegistry);
    }

    @PostConstruct
    void announce() {
        log.info("[StuckTrackerSweeper] Active — stuckThresholdMinutes={}", stuckThresholdMinutes);
    }

    @Scheduled(fixedDelayString    = "${app.sweeper.interval-ms:60000}",
               initialDelayString  = "${app.sweeper.initial-delay-ms:30000}")
    @Transactional
    public void sweep() {
        sweepRunCounter.increment();
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(stuckThresholdMinutes);

        List<JobTracker> candidates = repo.findStuckTrackers(cutoff);
        if (candidates.isEmpty()) return;

        log.info("[StuckTrackerSweeper] Inspecting {} candidate tracker(s)", candidates.size());

        int reconciled = 0;
        for (JobTracker tracker : candidates) {
            try {
                if (reconcile(tracker)) reconciled++;
            } catch (Exception e) {
                // Never let one bad tracker abort the whole sweep.
                log.error("[StuckTrackerSweeper] Reconciliation failed for tracker {}: {}",
                        tracker.getId(), e.getMessage(), e);
            }
        }

        if (reconciled > 0) {
            log.warn("[StuckTrackerSweeper] Reconciled {}/{} stuck tracker(s)",
                    reconciled, candidates.size());
            reconciledCounter.increment(reconciled);
        }
    }

    /** @return {@code true} if the tracker was modified in this pass */
    private boolean reconcile(JobTracker tracker) {
        Long executionId = tracker.getExecutionId();
        JobExecution execution = (executionId != null) ? jobExplorer.getJobExecution(executionId) : null;

        if (execution != null && execution.isRunning()) {
            log.debug("[StuckTrackerSweeper] Tracker {} has live JobExecution {} — leaving alone",
                    tracker.getId(), executionId);
            return false;
        }

        if (execution != null) {
            BatchStatus realStatus = execution.getStatus();
            log.warn("[StuckTrackerSweeper] Tracker {} stuck at {} but JobExecution {} is {} — syncing",
                    tracker.getId(), tracker.getStatus(), executionId, realStatus);

            tracker.setStatus(realStatus.toString());
            String reason = (realStatus == BatchStatus.COMPLETED)
                    ? "Sweeper-reconciled: completion listener crashed before persisting tracker"
                    : "Sweeper-reconciled: " + execution.getExitStatus().getExitDescription();
            tracker.setErrorMessage(reason);

            if (realStatus == BatchStatus.COMPLETED && "IMPORT".equals(tracker.getJobType())) {
                replayImportCleanup(tracker);
            }
        } else {
            log.warn("[StuckTrackerSweeper] Tracker {} stuck at {} with no JobExecution — marking ABANDONED",
                    tracker.getId(), tracker.getStatus());
            tracker.setStatus("ABANDONED");
            tracker.setErrorMessage("Sweeper-reconciled: no JobExecution after " +
                    stuckThresholdMinutes + " minute(s) in " + tracker.getStatus() +
                    ". The original upload is preserved at " + tracker.getFilePath() +
                    " — re-submit if needed.");
        }

        tracker.setUpdatedAt(LocalDateTime.now());
        repo.save(tracker);
        return true;
    }

    /**
     * Idempotent replay of the listener's storage cleanup. If the listener
     * already deleted the durable file, {@code storage.delete} is a no-op for
     * both local (Files.deleteIfExists) and S3 (DELETE on missing key is silent).
     */
    private void replayImportCleanup(JobTracker tracker) {
        String location = tracker.getFilePath();
        if (location == null) return;
        try {
            storage.delete(location);
            log.info("[StuckTrackerSweeper] Replayed cleanup of: {}", location);
        } catch (IOException e) {
            log.warn("[StuckTrackerSweeper] Cleanup replay failed for {}: {}",
                    location, e.getMessage());
        }
    }
}
