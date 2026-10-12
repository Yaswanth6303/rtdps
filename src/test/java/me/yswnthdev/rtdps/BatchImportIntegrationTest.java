package me.yswnthdev.rtdps;

import com.fasterxml.jackson.databind.ObjectMapper;
import me.yswnthdev.rtdps.dto.JobRequest;
import me.yswnthdev.rtdps.entity.JobTracker;
import me.yswnthdev.rtdps.repository.JobTrackerRepository;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end pipeline test — exercises the full happy path:
 *
 * <pre>
 *   HTTP multipart upload
 *      → JobController persists the tracker + publishes JobRequest
 *      → Embedded Kafka delivers to JobRequestConsumer
 *      → BatchJobService.launchImportJob → async TaskExecutorJobLauncher
 *      → Spring Batch multi-threaded chunk step
 *      → JdbcBatchItemWriter with ON CONFLICT DO NOTHING → real Postgres (Testcontainers)
 *      → JobCompletionListener updates tracker to COMPLETED
 * </pre>
 *
 * The test asserts:
 * <ol>
 *   <li>The controller returns {@code 202 Accepted} with a tracker ID</li>
 *   <li>The tracker eventually transitions to {@code COMPLETED} (polled with Awaitility)</li>
 *   <li>Every row in the source CSV lands in the {@code customers} table</li>
 *   <li>Tracker counters (read / write / skip) are rolled up correctly</li>
 * </ol>
 *
 * Requires a container runtime (Podman or Docker). On macOS with Podman:
 * <pre>
 *   export DOCKER_HOST="unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"
 * </pre>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@Testcontainers
@EmbeddedKafka(
        partitions = 1,
        topics = {"job-requests", "job-events", "job-requests.DLT"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@TestPropertySource(properties = {
        // Sandbox uploads so parallel test runs don't clobber each other.
        "app.upload-dir=${java.io.tmpdir}/rtdps-it-uploads"
})
class BatchImportIntegrationTest {

    /** @ServiceConnection auto-wires spring.datasource.* to this container — no @DynamicPropertySource needed. */
    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17");

    @Autowired MockMvc mockMvc;
    @Autowired JobTrackerRepository trackerRepo;
    @Autowired JdbcTemplate jdbcTemplate;
    @Autowired ObjectMapper objectMapper;
    @Autowired KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired EmbeddedKafkaBroker embeddedKafka;

    @Test
    @DisplayName("CSV upload → Kafka → async batch → rows landed in customers")
    void csvUpload_endToEnd() throws Exception {
        String csv = """
                id,firstName,lastName,email,gender,contactNo,country,dob
                1,Ada,Lovelace,ada@example.com,F,555-111,UK,1815-12-10
                2,Alan,Turing,alan@example.com,M,555-222,UK,1912-06-23
                3,Grace,Hopper,grace@example.com,F,555-333,USA,1906-12-09
                """;
        MockMultipartFile file = new MockMultipartFile(
                "file", "customers.csv", "text/csv", csv.getBytes(StandardCharsets.UTF_8));

        MvcResult result = mockMvc.perform(multipart("/jobs/importCustomers").file(file))
                .andExpect(status().isAccepted())
                .andReturn();

        @SuppressWarnings("unchecked")
        Map<String, Object> body = objectMapper.readValue(
                result.getResponse().getContentAsString(), Map.class);
        String trackerId = (String) body.get("trackerId");
        assertThat(trackerId).as("controller must return trackerId").isNotNull();

        // The launcher is async, so poll the tracker until it hits a terminal state.
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    JobTracker t = trackerRepo.findById(trackerId).orElseThrow();
                    assertThat(t.getStatus()).isEqualTo("COMPLETED");
                });

        // All 3 rows made it into customers (idempotency guarantees exactly once).
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM customers WHERE id IN (1, 2, 3)", Integer.class);
        assertThat(count).as("rows persisted").isEqualTo(3);

        // Counters were rolled up correctly by JobCompletionListener.
        JobTracker tracker = trackerRepo.findById(trackerId).orElseThrow();
        assertThat(tracker.getReadCount()).isEqualTo(3);
        assertThat(tracker.getWriteCount()).isEqualTo(3);
        assertThat(tracker.getSkipCount()).isZero();
    }

    @Test
    @DisplayName("Request for a missing file → non-retryable → DLT → tracker FAILED")
    void missingFile_routedToDlt() {
        String trackerId = saveQueuedTracker("/nonexistent/customers.csv");

        kafkaTemplate.send("job-requests", trackerId, JobRequest.builder()
                .trackerId(trackerId)
                .jobType("IMPORT")
                .filePath("/nonexistent/customers.csv")
                .requestedAt(LocalDateTime.now())
                .build());

        awaitFailedViaDlt(trackerId);
    }

    @Test
    @DisplayName("Malformed JSON payload → DeserializationException → DLT → tracker FAILED")
    void malformedPayload_routedToDlt() {
        String trackerId = saveQueuedTracker("/irrelevant.csv");

        Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaTemplate<String, String> rawTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
        rawTemplate.send("job-requests", trackerId, "{not valid json");

        awaitFailedViaDlt(trackerId);
    }

    private String saveQueuedTracker(String filePath) {
        String id = UUID.randomUUID().toString();
        trackerRepo.save(JobTracker.builder()
                .id(id)
                .jobType("IMPORT")
                .filePath(filePath)
                .status("QUEUED")
                .createdAt(LocalDateTime.now())
                .build());
        return id;
    }

    private void awaitFailedViaDlt(String trackerId) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    JobTracker t = trackerRepo.findById(trackerId).orElseThrow();
                    assertThat(t.getStatus()).isEqualTo("FAILED");
                    assertThat(t.getErrorMessage()).startsWith("Sent to DLT");
                });
    }
}
