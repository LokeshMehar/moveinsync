package com.moveinsync.scheduler.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.api.GlobalExceptionHandler.ErrorBody;
import com.moveinsync.scheduler.security.JwtAuthFilter;
import com.moveinsync.scheduler.security.JwtService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import java.time.Instant;
import java.util.List;

@Configuration
@EnableMethodSecurity
public class SecurityConfig {

    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder(10);
    }

    @Bean
    SecurityFilterChain chain(HttpSecurity http, JwtService jwt, ObjectMapper mapper) throws Exception {
        http.csrf(c -> c.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(a -> a
                        .requestMatchers("/auth/token", "/actuator/health/**", "/actuator/info").permitAll()
                        .requestMatchers("/actuator/**").hasRole("OPERATOR")
                        .requestMatchers("/api/**").authenticated()
                        .anyRequest().denyAll())
                .exceptionHandling(e -> e
                        .authenticationEntryPoint((req, res, ex) -> write(res, mapper, HttpStatus.UNAUTHORIZED, "UNAUTHENTICATED", "authentication required"))
                        .accessDeniedHandler((req, res, ex) -> write(res, mapper, HttpStatus.FORBIDDEN, "FORBIDDEN", "not allowed")))
                .addFilterBefore(new JwtAuthFilter(jwt), UsernamePasswordAuthenticationFilter.class);
        return http.build();
    }

    private static void write(jakarta.servlet.http.HttpServletResponse res, ObjectMapper mapper, HttpStatus status, String code, String msg)
            throws java.io.IOException {
        res.setStatus(status.value());
        res.setContentType(MediaType.APPLICATION_JSON_VALUE);
        mapper.writeValue(res.getOutputStream(), new ErrorBody(code, msg, List.of(), null, Instant.now()));
    }
}
