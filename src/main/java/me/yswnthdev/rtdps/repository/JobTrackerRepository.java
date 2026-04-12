package me.yswnthdev.rtdps.repository;

import me.yswnthdev.rtdps.entity.JobTracker;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface JobTrackerRepository extends JpaRepository<JobTracker, String> {
    List<JobTracker> findAllByOrderByCreatedAtDesc();
}
