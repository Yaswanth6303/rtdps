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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Batch Jobs", description = "Endpoints for managing async batch import jobs via Kafka")
public class JobController {

    private final JobRequestProducer jobRequestProducer;
    private final JobTrackerRepository jobTrackerRepository;

    @Value("${app.upload-dir}")
    private String uploadDir;

    @Operation(summary = "Import CSV to Database (Async)",
            description = "Upload a CSV file. The import job is queued via Kafka and processed asynchronously. " +
                    "Use the returned tracker ID to poll job status.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Job request accepted and queued"),
            @ApiResponse(responseCode = "400", description = "Invalid file"),
            @ApiResponse(responseCode = "500", description = "Failed to store uploaded file")
    })
    @PostMapping(value = "/importCustomers", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> importCsvToDBJob(
            @Parameter(description = "CSV file to import")
            @RequestParam("file") MultipartFile file) {

        log.info("[JobController] Import request received — file: {}, size: {} bytes",
                file.getOriginalFilename(), file.getSize());

        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "File is empty"));
        }

        String originalFilename = file.getOriginalFilename();
        String contentType = file.getContentType();
        if (originalFilename == null
                || (!originalFilename.toLowerCase().endsWith(".csv") && !"text/csv".equals(contentType))) {
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid file type. Only CSV files are allowed."));
        }

        try {
            Path uploadPath = Paths.get(uploadDir);
            Files.createDirectories(uploadPath);
            String storedName = System.currentTimeMillis() + "_" + originalFilename;
            Path target = uploadPath.resolve(storedName);
            Files.copy(file.getInputStream(), target, StandardCopyOption.REPLACE_EXISTING);
            log.info("[JobController] File stored at: {}", target);

            JobTracker tracker = JobTracker.builder()
                    .id(UUID.randomUUID().toString())
                    .jobType("IMPORT")
                    .filePath(target.toString())
                    .status("QUEUED")
                    .createdAt(LocalDateTime.now())
                    .build();
            jobTrackerRepository.save(tracker);

            JobRequest request = JobRequest.builder()
                    .trackerId(tracker.getId())
                    .jobType("IMPORT")
                    .filePath(target.toString())
                    .requestedAt(LocalDateTime.now())
                    .build();
            jobRequestProducer.sendJobRequest(request);

            return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                    "message", "Import job queued successfully",
                    "trackerId", tracker.getId(),
                    "status", "QUEUED",
                    "filePath", target.toString()
            ));

        } catch (IOException e) {
            log.error("[JobController] Failed to store uploaded file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "error", "Failed to store uploaded file: " + e.getMessage()
            ));
        }
    }

    @Operation(summary = "Get Job Status")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job status retrieved"),
            @ApiResponse(responseCode = "404", description = "Tracker not found")
    })
    @GetMapping("/status/{trackerId}")
    public ResponseEntity<?> getJobStatus(
            @Parameter(description = "Job tracker ID returned from import endpoint")
            @PathVariable String trackerId) {

        return jobTrackerRepository.findById(trackerId)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                        "error", "No job found with tracker ID: " + trackerId
                )));
    }

    @Operation(summary = "List All Jobs")
    @GetMapping("/all")
    public ResponseEntity<List<JobTracker>> listAllJobs() {
        return ResponseEntity.ok(jobTrackerRepository.findAllByOrderByCreatedAtDesc());
    }
}
