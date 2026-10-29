package com.moveinsync.scheduler.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;

public class JwtService {

    private final SecretKey key;
    private final SecurityProps props;

    public JwtService(SecurityProps props) {
        this.props = props;
        this.key = Keys.hmacShaKeyFor(props.jwtSecret().getBytes(StandardCharsets.UTF_8));
    }

    public String issue(Caller caller) {
        Instant now = Instant.now();
        var builder = Jwts.builder().subject(caller.username()).claim("role", caller.role().name())
                .issuedAt(Date.from(now)).expiration(Date.from(now.plus(props.jwtTtl())));
        if (caller.tenantId() != null) {
            builder.claim("tenant", caller.tenantId());
        }
        return builder.signWith(key).compact();
    }

    public Optional<Caller> parse(String token) {
        try {
            Claims c = Jwts.parser().verifyWith(key).build().parseSignedClaims(token).getPayload();
            return Optional.of(new Caller(c.getSubject(), Caller.Role.valueOf(c.get("role", String.class)), c.get("tenant", String.class)));
        } catch (JwtException | IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
