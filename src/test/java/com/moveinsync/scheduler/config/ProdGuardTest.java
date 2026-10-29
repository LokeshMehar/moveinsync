package com.moveinsync.scheduler.config;

import com.moveinsync.scheduler.security.SecurityProps;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProdGuardTest {

    private static SecurityProps withSecret(String secret) {
        return new SecurityProps(secret, Duration.ofHours(1), List.of(), List.of());
    }

    @Test
    void prodProfileRefusesToSignTokensWithThePublicDefaultSecret() {
        MockEnvironment prod = new MockEnvironment();
        prod.setActiveProfiles("prod");
        assertThatThrownBy(() -> new AppConfig().jwtService(withSecret("dev-only-secret-change-me-dev-only-secret-change-me"), prod))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("JWT_SECRET");
        assertThat(new AppConfig().jwtService(withSecret("a-real-secret-that-is-at-least-32-bytes-long"), prod)).isNotNull();
    }

    @Test
    void localDevKeepsWorkingWithTheDefaultSecret() {
        assertThat(new AppConfig().jwtService(withSecret("dev-only-secret-change-me-dev-only-secret-change-me"), new MockEnvironment())).isNotNull();
    }
}
