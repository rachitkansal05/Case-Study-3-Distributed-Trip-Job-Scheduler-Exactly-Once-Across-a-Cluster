package com.moveinsync.scheduler.app.error;

import com.fasterxml.jackson.databind.JsonMappingException;
import jakarta.validation.ConstraintViolationException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.validation.FieldError;
import org.springframework.web.ErrorResponse;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/**
 * Maps every exception to an RFC 7807 {@link ProblemDetail} with a stable {@code code}, a safe
 * message and the request's {@code traceId} (docs/design/architecture.md section 14).
 *
 * <p>Responses never contain stack traces, SQL, exception messages of libraries or rejected
 * values: validation errors name the field and a fixed reason only. Unexpected errors are logged
 * (masked by the PII converters) and answered with a generic 500.
 */
@RestControllerAdvice
public class ProblemDetailAdvice {

    private static final Logger log = LoggerFactory.getLogger(ProblemDetailAdvice.class);
    private static final String IDEMPOTENCY_HEADER = "Idempotency-Key";

    @ExceptionHandler(SchedulerException.class)
    ResponseEntity<ProblemDetail> scheduler(SchedulerException e) {
        if (e instanceof StoreUnavailableException) {
            log.warn("scheduler store unavailable: {}", e.getCause() == null ? "-" : e.getCause().getClass().getName());
        }
        return respond(Problems.of(e));
    }

    // ------------------------------------------------------------------ request validation

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ProblemDetail> invalidBody(MethodArgumentNotValidException e) {
        List<ValidationException.FieldIssue> issues = new ArrayList<>();
        for (FieldError fe : e.getBindingResult().getFieldErrors()) {
            issues.add(new ValidationException.FieldIssue(fe.getField(), reason(fe.getDefaultMessage())));
        }
        e.getBindingResult().getGlobalErrors().forEach(ge ->
                issues.add(new ValidationException.FieldIssue(ge.getObjectName(), reason(ge.getDefaultMessage()))));
        return validation(issues);
    }

    @ExceptionHandler(HandlerMethodValidationException.class)
    ResponseEntity<ProblemDetail> invalidParameter(HandlerMethodValidationException e) {
        List<ValidationException.FieldIssue> issues = new ArrayList<>();
        e.getAllValidationResults().forEach(r -> {
            String name = r.getMethodParameter().getParameterName();
            r.getResolvableErrors().forEach(err -> issues.add(new ValidationException.FieldIssue(
                    name == null ? "parameter" : name, reason(err.getDefaultMessage()))));
        });
        return validation(issues);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    ResponseEntity<ProblemDetail> constraintViolation(ConstraintViolationException e) {
        List<ValidationException.FieldIssue> issues = e.getConstraintViolations().stream()
                .map(v -> {
                    String path = v.getPropertyPath().toString();
                    String field = path.contains(".") ? path.substring(path.lastIndexOf('.') + 1) : path;
                    return new ValidationException.FieldIssue(field, reason(v.getMessage()));
                })
                .toList();
        return validation(issues);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ProblemDetail> unreadable(HttpMessageNotReadableException e) {
        if (e.getCause() instanceof JsonMappingException jme && !jme.getPath().isEmpty()) {
            String field = jme.getPath().stream()
                    .map(ref -> ref.getFieldName() != null ? ref.getFieldName() : "[" + ref.getIndex() + "]")
                    .collect(Collectors.joining("."));
            return respond(Problems.of(ValidationException.field(field, "has an invalid value or type")));
        }
        return respond(Problems.of(HttpStatus.BAD_REQUEST, ErrorCodes.MALFORMED_REQUEST,
                "Request body is missing or is not valid JSON"));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    ResponseEntity<ProblemDetail> typeMismatch(MethodArgumentTypeMismatchException e) {
        return respond(Problems.of(ValidationException.field(e.getName(), "has an invalid format")));
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    ResponseEntity<ProblemDetail> missingHeader(MissingRequestHeaderException e) {
        if (IDEMPOTENCY_HEADER.equalsIgnoreCase(e.getHeaderName())) {
            return respond(Problems.of(new ValidationException(ErrorCodes.IDEMPOTENCY_KEY_REQUIRED,
                    "Idempotency-Key header is required",
                    List.of(new ValidationException.FieldIssue(IDEMPOTENCY_HEADER, "is required")))));
        }
        return respond(Problems.of(ValidationException.field(e.getHeaderName(), "is required")));
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    ResponseEntity<ProblemDetail> missingParameter(MissingServletRequestParameterException e) {
        return respond(Problems.of(ValidationException.field(e.getParameterName(), "is required")));
    }

    // ------------------------------------------------------------------ routing and protocol

    @ExceptionHandler({NoResourceFoundException.class, NoHandlerFoundException.class})
    ResponseEntity<ProblemDetail> noRoute(Exception e) {
        return respond(Problems.of(HttpStatus.NOT_FOUND, ErrorCodes.NOT_FOUND, "Resource not found"));
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    ResponseEntity<ProblemDetail> methodNotAllowed(HttpRequestMethodNotSupportedException e) {
        return respond(Problems.of(HttpStatus.METHOD_NOT_ALLOWED, ErrorCodes.METHOD_NOT_ALLOWED,
                "HTTP method not supported for this resource"));
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    ResponseEntity<ProblemDetail> unsupportedMediaType(HttpMediaTypeNotSupportedException e) {
        return respond(Problems.of(HttpStatus.UNSUPPORTED_MEDIA_TYPE, ErrorCodes.UNSUPPORTED_MEDIA_TYPE,
                "Content type not supported; use application/json"));
    }

    @ExceptionHandler(HttpMediaTypeNotAcceptableException.class)
    ResponseEntity<ProblemDetail> notAcceptable(HttpMediaTypeNotAcceptableException e) {
        return respond(Problems.of(HttpStatus.NOT_ACCEPTABLE, ErrorCodes.NOT_ACCEPTABLE,
                "Requested media type not available"));
    }

    @ExceptionHandler(AccessDeniedException.class)
    ResponseEntity<ProblemDetail> accessDenied(AccessDeniedException e) {
        return respond(Problems.of(HttpStatus.FORBIDDEN, ErrorCodes.FORBIDDEN, "Access denied"));
    }

    // ------------------------------------------------------------------ database

    @ExceptionHandler({DataAccessResourceFailureException.class, TransientDataAccessResourceException.class,
            CannotCreateTransactionException.class})
    ResponseEntity<ProblemDetail> storeUnavailable(Exception e) {
        return scheduler(new StoreUnavailableException(e));
    }

    @ExceptionHandler({DataIntegrityViolationException.class, ConcurrencyFailureException.class})
    ResponseEntity<ProblemDetail> concurrentModification(Exception e) {
        log.info("request rejected by a database constraint or lock conflict: {}", e.getClass().getName());
        return respond(Problems.of(HttpStatus.CONFLICT, ErrorCodes.CONCURRENT_MODIFICATION,
                "The resource was modified concurrently; retry the request"));
    }

    // ------------------------------------------------------------------ fallbacks

    /**
     * Last resort. Other Spring MVC exceptions carry their own 4xx status ({@link ErrorResponse});
     * they keep the status but get a generic message. Everything else is a 500.
     */
    @ExceptionHandler(Exception.class)
    ResponseEntity<ProblemDetail> unexpected(Exception e) {
        if (e instanceof ErrorResponse er) {
            HttpStatus status = HttpStatus.resolve(er.getStatusCode().value());
            if (status != null && status.is4xxClientError()) {
                return respond(Problems.of(status, ErrorCodes.MALFORMED_REQUEST, status.getReasonPhrase()));
            }
        }
        log.error("unexpected error while handling a request", e);
        return respond(Problems.of(HttpStatus.INTERNAL_SERVER_ERROR, ErrorCodes.INTERNAL_ERROR,
                "Internal error; quote the traceId when reporting it"));
    }

    // ------------------------------------------------------------------ helpers

    private ResponseEntity<ProblemDetail> validation(List<ValidationException.FieldIssue> issues) {
        return respond(Problems.of(new ValidationException(ErrorCodes.VALIDATION_FAILED,
                "Request validation failed", issues)));
    }

    private static ResponseEntity<ProblemDetail> respond(ProblemDetail problem) {
        return ResponseEntity.status(problem.getStatus()).body(problem);
    }

    private static String reason(String defaultMessage) {
        return defaultMessage == null ? "is invalid" : defaultMessage;
    }
}
