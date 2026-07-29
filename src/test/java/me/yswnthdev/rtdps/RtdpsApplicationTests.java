package me.yswnthdev.rtdps;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Fast context-load smoke test — boots with H2 + Kafka autoconfig excluded so
 * nothing external needs to be running. The @ActiveProfiles("mock") pulls in
 * application-mock.yml; the integration tests use the real main/application.yml.
 */
@SpringBootTest
@ActiveProfiles("mock")
class RtdpsApplicationTests {

	@MockitoBean
	private KafkaTemplate<String, Object> kafkaTemplate;

	@Test
	void contextLoads() {
	}

}
