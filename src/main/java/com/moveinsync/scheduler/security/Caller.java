package com.moveinsync.scheduler.security;

import com.moveinsync.scheduler.api.ApiException;
import org.springframework.http.HttpStatus;

/**
 * The authenticated principal. {@code tenantId} is null only for OPERATOR (cross-tenant). Every data access goes
 * through {@link #scope(String)}, so a non-operator can never address another tenant, whatever they put in a request.
 */
public record Caller(String username, Role role, String tenantId) {

    public enum Role { EMPLOYEE, ADMIN, OPERATOR }

    public String scope(String requestedTenant) {
        if (role == Role.OPERATOR) {
            if (requestedTenant == null || requestedTenant.isBlank()) {
                throw new ApiException(HttpStatus.BAD_REQUEST, "TENANT_REQUIRED", "operators must name a tenant");
            }
            return requestedTenant;
        }
        if (requestedTenant != null && !requestedTenant.isBlank() && !requestedTenant.equals(tenantId)) {
            throw new ApiException(HttpStatus.NOT_FOUND, "NOT_FOUND", "resource not found");
        }
        return tenantId;
    }

    public void requireWriter() {
        if (role == Role.EMPLOYEE) {
            throw new ApiException(HttpStatus.FORBIDDEN, "FORBIDDEN", "not allowed");
        }
    }
}
