package com.moveinsync.scheduler.security;

import com.moveinsync.scheduler.api.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/auth")
public class AuthController {

    public record LoginRequest(String username, String password) {
    }

    // Compared when the user does not exist, so response time does not reveal valid usernames.
    private final String dummyHash;

    private final JdbcTemplate jdbc;
    private final PasswordEncoder encoder;
    private final JwtService jwt;

    public AuthController(JdbcTemplate jdbc, PasswordEncoder encoder, JwtService jwt) {
        this.jdbc = jdbc;
        this.encoder = encoder;
        this.jwt = jwt;
        this.dummyHash = encoder.encode("timing-equaliser");
    }

    @PostMapping("/token")
    public Map<String, Object> token(@RequestBody LoginRequest req) {
        if (req == null || req.username() == null || req.password() == null) {
            throw new ApiException(HttpStatus.UNAUTHORIZED, "BAD_CREDENTIALS", "invalid credentials");
        }
        List<Map<String, Object>> rows = jdbc.queryForList("SELECT password_hash, role, tenant_id FROM app_user WHERE username = ?", req.username());
        String hash = rows.isEmpty() ? dummyHash : (String) rows.get(0).get("password_hash");
        boolean ok = encoder.matches(req.password(), hash) && !rows.isEmpty();
        if (!ok) {
            throw new ApiException(HttpStatus.UNAUTHORIZED, "BAD_CREDENTIALS", "invalid credentials");
        }
        Caller caller = new Caller(req.username(), Caller.Role.valueOf((String) rows.get(0).get("role")), (String) rows.get(0).get("tenant_id"));
        return Map.of("token", jwt.issue(caller), "role", caller.role(), "tenant", String.valueOf(caller.tenantId()));
    }
}
