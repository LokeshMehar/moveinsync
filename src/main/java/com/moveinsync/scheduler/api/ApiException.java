package com.moveinsync.scheduler.api;

import org.springframework.http.HttpStatus;

/** An error whose message is safe to show to the client (never contains personal data or internals). */
public class ApiException extends RuntimeException {

    private final HttpStatus status;
    private final String code;

    public ApiException(HttpStatus status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public HttpStatus status() {
        return status;
    }

    public String code() {
        return code;
    }

    public static ApiException notFound() {
        return new ApiException(HttpStatus.NOT_FOUND, "NOT_FOUND", "resource not found");
    }
}
