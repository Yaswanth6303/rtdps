package me.yswnthdev.rtdps.repository;

import me.yswnthdev.rtdps.entity.JobTracker;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface JobTrackerRepository extends JpaRepository<JobTracker, String> {

    List<JobTracker> findAllByOrderByCreatedAtDesc();

    /**
     * Returns trackers stuck in {@code QUEUED} or {@code PROCESSING} whose last
     * observed activity (updatedAt if present, else createdAt) is older than
     * {@code cutoff}. Used by {@code StuckTrackerSweeper} to find orphaned
     * trackers — the consumer crashed before launching the job, or the
     * completion listener crashed before persisting the terminal state.
     */
    @Query("""
            SELECT t FROM JobTracker t
            WHERE t.status IN ('QUEUED', 'PROCESSING')
              AND COALESCE(t.updatedAt, t.createdAt) < :cutoff
            """)
    List<JobTracker> findStuckTrackers(@Param("cutoff") LocalDateTime cutoff);
}
