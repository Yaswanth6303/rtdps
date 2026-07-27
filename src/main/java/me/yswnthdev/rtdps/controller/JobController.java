package me.yswnthdev.rtdps.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.yswnthdev.rtdps.dto.JobRequest;
import me.yswnthdev.rtdps.entity.JobTracker;
import me.yswnthdev.rtdps.kafka.JobRequestProducer;
import me.yswnthdev.rtdps.repository.JobTrackerRepository;
import me.yswnthdev.rtdps.service.ConflictPolicy;
import me.yswnthdev.rtdps.storage.StorageBackend;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Batch Jobs", description = "Endpoints for managing async batch import/export jobs via Kafka")
public class JobController {

    private final JobRequestProducer jobRequestProducer;
    private final JobTrackerRepository jobTrackerRepository;
    private final StorageBackend storage;

    // ======================== IMPORT (Async via Kafka) ========================

    @Operation(summary = "Import CSV to Database (Async)",
            description = "Upload a CSV file. The import job is queued via Kafka and processed asynchronously. " +
                    "Use the returned tracker ID to poll job status. " +
                    "conflictPolicy=SKIP (default) keeps existing rows untouched and surfaces a conflictCount; " +
                    "conflictPolicy=UPDATE overwrites existing rows column-for-column.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Job request accepted and queued"),
            @ApiResponse(responseCode = "400", description = "Invalid file or conflictPolicy"),
            @ApiResponse(responseCode = "500", description = "Failed to process request")
    })
    @PostMapping(value = "/importCustomers", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> importCsvToDBJob(
            @Parameter(description = "CSV file to import")
            @RequestParam("file") MultipartFile file,
            @Parameter(description = "How to handle rows whose id already exists. SKIP (default) or UPDATE.")
            @RequestParam(value = "conflictPolicy", defaultValue = "SKIP") String conflictPolicy) {

        log.info("[JobController] Import request received — file: {}, size: {} bytes, conflictPolicy: {}",
                file.getOriginalFilename(), file.getSize(), conflictPolicy);

        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "File is empty"));
        }

        String originalFilename = file.getOriginalFilename();
        String contentType = file.getContentType();
        if (originalFilename == null
                || (!originalFilename.toLowerCase().endsWith(".csv") && !"text/csv".equals(contentType))) {
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid file type. Only CSV files are allowed."));
        }

        ConflictPolicy policy;
        try {
            policy = ConflictPolicy.valueOf(conflictPolicy.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "Invalid conflictPolicy: " + conflictPolicy + ". Must be SKIP or UPDATE."));
        }

        try {
            String location = storage.store(file.getInputStream(), originalFilename);
            log.info("[JobController] File stored at: {}", location);

            JobTracker tracker = JobTracker.builder()
                    .id(UUID.randomUUID().toString())
                    .jobType("IMPORT")
                    .filePath(location)
                    .status("QUEUED")
                    .createdAt(LocalDateTime.now())
                    .build();
            jobTrackerRepository.save(tracker);

            JobRequest request = JobRequest.builder()
                    .trackerId(tracker.getId())
                    .jobType("IMPORT")
                    .filePath(location)
                    .conflictPolicy(policy.name())
                    .requestedAt(LocalDateTime.now())
                    .build();
            jobRequestProducer.sendJobRequest(request);

            return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                    "message", "Import job queued successfully",
                    "trackerId", tracker.getId(),
                    "status", "QUEUED",
                    "location", location,
                    "conflictPolicy", policy.name()
            ));

        } catch (IOException e) {
            log.error("[JobController] Failed to store uploaded file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "error", "Failed to store uploaded file: " + e.getMessage()
            ));
        }
    }

    // ======================== EXPORT (Async via Kafka) ========================

    @Operation(summary = "Export Database to CSV (Async)",
            description = "Queues an export job via Kafka. Once complete, download the file using " +
                    "GET /jobs/download/{trackerId}.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Export job queued"),
            @ApiResponse(responseCode = "500", description = "Failed to queue export")
    })
    @PostMapping("/exportCustomers")
    public ResponseEntity<Map<String, Object>> exportCustomersToCSV() {
        log.info("[JobController] Export request received");

        JobTracker tracker = JobTracker.builder()
                .id(UUID.randomUUID().toString())
                .jobType("EXPORT")
                .status("QUEUED")
                .createdAt(LocalDateTime.now())
                .build();
        jobTrackerRepository.save(tracker);

        JobRequest request = JobRequest.builder()
                .trackerId(tracker.getId())
                .jobType("EXPORT")
                .requestedAt(LocalDateTime.now())
                .build();
        jobRequestProducer.sendJobRequest(request);

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "message", "Export job queued successfully",
                "trackerId", tracker.getId(),
                "status", "QUEUED"
        ));
    }

    // ======================== STATUS ========================

    @Operation(summary = "Get Job Status",
            description = "Returns the current status and metrics of a tracked job by its tracker ID.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job status retrieved"),
            @ApiResponse(responseCode = "404", description = "Tracker not found")
    })
    @GetMapping("/status/{trackerId}")
    public ResponseEntity<?> getJobStatus(
            @Parameter(description = "Job tracker ID returned from import/export endpoint")
            @PathVariable String trackerId) {

        log.info("[JobController] Status request for tracker: {}", trackerId);

        return jobTrackerRepository.findById(trackerId)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                        "error", "No job found with tracker ID: " + trackerId
                )));
    }

    // ======================== DOWNLOAD ========================

    @Operation(summary = "Download Exported CSV",
            description = "Downloads the exported CSV file once the export job is complete.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "CSV file download"),
            @ApiResponse(responseCode = "202", description = "Export still processing"),
            @ApiResponse(responseCode = "404", description = "Tracker not found or not an export job")
    })
    @GetMapping("/download/{trackerId}")
    public ResponseEntity<?> downloadExport(
            @Parameter(description = "Tracker ID of the export job")
            @PathVariable String trackerId) {

        log.info("[JobController] Download request for tracker: {}", trackerId);

        JobTracker tracker = jobTrackerRepository.findById(trackerId).orElse(null);

        if (tracker == null || !"EXPORT".equals(tracker.getJobType())) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                    "error", "No export job found with tracker ID: " + trackerId
            ));
        }

        if (!"COMPLETED".equals(tracker.getStatus()) || tracker.getFilePath() == null) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                    "message", "Export is still processing",
                    "status", tracker.getStatus(),
                    "trackerId", trackerId
            ));
        }

        try {
            Resource resource = storage.asResource(tracker.getFilePath());
            String downloadName = "customers_" + trackerId + ".csv";
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + downloadName)
                    .contentType(MediaType.parseMediaType("text/csv"))
                    .body(resource);
        } catch (IOException e) {
            log.error("[JobController] Download failed for {}", trackerId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "error", "Export object unreadable: " + e.getMessage()
            ));
        }
    }

    // ======================== LIST ALL JOBS ========================

    @Operation(summary = "List All Jobs",
            description = "Returns all tracked jobs ordered by creation time (newest first).")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job list retrieved")
    })
    @GetMapping("/all")
    public ResponseEntity<List<JobTracker>> listAllJobs() {
        return ResponseEntity.ok(jobTrackerRepository.findAllByOrderByCreatedAtDesc());
    }
}
