package com.moveinsync.scheduler.sink.web;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/**
 * Renders every error as an RFC 7807 ProblemDetail with a stable {@code code} property. Bodies
 * contain only fixed texts: no stack traces, exception messages or request values (not even the
 * request path, so {@code instance} is omitted).
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(SinkProblemException.class)
    ResponseEntity<ProblemDetail> sinkProblem(SinkProblemException e) {
        ProblemDetail body = problem(e.code(), e.status(), e.getMessage());
        if (!e.violations().isEmpty()) {
            body.setProperty("errors", e.violations().stream()
                    .map(v -> Map.of("field", v.field(), "message", v.message()))
                    .toList());
        }
        if (e.code() == ProblemCode.STALE_TOKEN) {
            body.setProperty("outcome", "STALE_TOKEN");
        }
        return respond(body);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ProblemDetail> unreadable() {
        return respond(problem(ProblemCode.MALFORMED_BODY, "The request body could not be read."));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    ResponseEntity<ProblemDetail> typeMismatch(MethodArgumentTypeMismatchException e) {
        return validation(e.getName(), "has the wrong type");
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    ResponseEntity<ProblemDetail> missingParameter(MissingServletRequestParameterException e) {
        return validation(e.getParameterName(), "is required");
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    ResponseEntity<ProblemDetail> unsupportedMediaType() {
        return respond(problem(ProblemCode.UNSUPPORTED_MEDIA_TYPE, "Send the request body as application/json."));
    }

    @ExceptionHandler(HttpMediaTypeNotAcceptableException.class)
    ResponseEntity<ProblemDetail> notAcceptable() {
        return respond(problem(ProblemCode.NOT_ACCEPTABLE, "Responses are application/json."));
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    ResponseEntity<ProblemDetail> methodNotAllowed() {
        return respond(problem(ProblemCode.METHOD_NOT_ALLOWED, "This method is not supported here."));
    }

    @ExceptionHandler(NoResourceFoundException.class)
    ResponseEntity<ProblemDetail> notFound() {
        return respond(problem(ProblemCode.NOT_FOUND, "No such endpoint."));
    }

    @ExceptionHandler({CannotGetJdbcConnectionException.class, CannotCreateTransactionException.class,
            DataAccessResourceFailureException.class, TransientDataAccessException.class})
    ResponseEntity<ProblemDetail> storeUnavailable(Exception e) {
        log.warn("sink store unavailable type={}", e.getClass().getName());
        return respond(problem(ProblemCode.STORE_UNAVAILABLE, "The sink database is unavailable; retry later."));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ProblemDetail> unexpected(Exception e) {
        // Class name only: messages of driver or Jackson exceptions may quote request content.
        log.error("unexpected error type={}", e.getClass().getName());
        return respond(problem(ProblemCode.INTERNAL_ERROR, "An unexpected error occurred."));
    }

    private static ResponseEntity<ProblemDetail> validation(String field, String message) {
        ProblemDetail body = problem(ProblemCode.VALIDATION_FAILED, "The request is invalid.");
        body.setProperty("errors", List.of(Map.of("field", field, "message", message)));
        return respond(body);
    }

    private static ProblemDetail problem(ProblemCode code, String detail) {
        return problem(code, code.status(), detail);
    }

    private static ProblemDetail problem(ProblemCode code, HttpStatus status, String detail) {
        ProblemDetail body = ProblemDetail.forStatusAndDetail(status, detail);
        body.setTitle(code.title());
        body.setProperty("code", code.name());
        return body;
    }

    private static ResponseEntity<ProblemDetail> respond(ProblemDetail body) {
        return ResponseEntity.status(body.getStatus()).body(body);
    }
}
