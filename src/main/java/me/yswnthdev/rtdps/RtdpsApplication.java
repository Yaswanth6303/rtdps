package me.yswnthdev.rtdps;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class RtdpsApplication {

	public static void main(String[] args) {
		SpringApplication.run(RtdpsApplication.class, args);
	}

}
