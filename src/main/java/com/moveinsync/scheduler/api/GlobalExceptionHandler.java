package com.moveinsync.scheduler.api;

import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * One error shape for the whole API. Messages are fixed strings or field names only: request bodies, payloads,
 * exception texts and stack traces never reach the client or the log line (unknown failures get an errorId that
 * links the response to a server-side stack trace).
 *
 * <p>Extends Spring's base advice so every framework-level problem (unknown route, wrong method, wrong media type,
 * unreadable body, bad path variable) keeps its proper 4xx status instead of falling into the catch-all 500.
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    public record ErrorBody(String code, String message, List<String> fields, String errorId, Instant timestamp) {
    }

    @ExceptionHandler(ApiException.class)
    ResponseEntity<ErrorBody> api(ApiException e) {
        return build(e.status(), e.code(), e.getMessage(), List.of(), null);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    ResponseEntity<ErrorBody> constraint(ConstraintViolationException e) {
        return build(HttpStatus.BAD_REQUEST, "BAD_REQUEST", "malformed or invalid request", List.of(), null);
    }

    @ExceptionHandler(AccessDeniedException.class)
    ResponseEntity<ErrorBody> denied(AccessDeniedException e) {
        return build(HttpStatus.FORBIDDEN, "FORBIDDEN", "not allowed", List.of(), null);
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ErrorBody> unexpected(Exception e) {
        String errorId = UUID.randomUUID().toString();
        log.error("unhandled error errorId={}", errorId, e);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "internal error", List.of(), errorId);
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException e, HttpHeaders headers,
                                                                  HttpStatusCode status, WebRequest request) {
        List<String> fields = e.getBindingResult().getFieldErrors().stream().map(f -> f.getField() + " " + f.getDefaultMessage()).toList();
        return new ResponseEntity<>(new ErrorBody("VALIDATION_FAILED", "request validation failed", fields, null, Instant.now()),
                headers, status);
    }

    /** Everything the base class maps (404, 405, 415, 400 ...) is rendered in our error shape, never with Spring's text. */
    @Override
    protected ResponseEntity<Object> handleExceptionInternal(Exception e, Object body, HttpHeaders headers,
                                                             HttpStatusCode status, WebRequest request) {
        HttpStatus http = HttpStatus.valueOf(status.value());
        String code = http == HttpStatus.BAD_REQUEST ? "BAD_REQUEST" : http.name();
        String message = http == HttpStatus.BAD_REQUEST ? "malformed or invalid request" : http.getReasonPhrase().toLowerCase();
        return new ResponseEntity<>(new ErrorBody(code, message, List.of(), null, Instant.now()), headers, status);
    }

    private static ResponseEntity<ErrorBody> build(HttpStatus s, String code, String msg, List<String> fields, String errorId) {
        return ResponseEntity.status(s).body(new ErrorBody(code, msg, fields, errorId, Instant.now()));
    }
}
