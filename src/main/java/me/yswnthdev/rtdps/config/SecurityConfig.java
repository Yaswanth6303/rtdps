package me.yswnthdev.rtdps.config;

import static org.springframework.security.config.Customizer.withDefaults;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain apiSecurity(HttpSecurity http) throws Exception {
        return http.csrf(csrf -> csrf.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(
                        auth ->
                                auth.requestMatchers(
                                        "/swagger-ui.html",
                                        "/swagger-ui/**",
                                        "/v3/api-docs/**",
                                        // K8s probes hit /actuator/health/liveness and
                                        // /readiness anonymously.
                                        "/actuator/health",
                                        "/actuator/health/**",
                                        "/actuator/info",
                                        // Prometheus scrape — restrict at the network layer
                                        // (in-cluster only).
                                        "/actuator/prometheus")
                                .permitAll()
                                .anyRequest()
                                .authenticated())
        .httpBasic(withDefaults())
        .build();
    }
}
