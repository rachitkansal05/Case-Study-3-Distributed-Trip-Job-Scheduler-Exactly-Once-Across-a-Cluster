package com.moveinsync.scheduler.app.error;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.MDC;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.MediaType;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Error responses never leak internals (PLAN.md section 14 "Error handling", R14): every exception
 * the advice can meet becomes an RFC 7807 {@code application/problem+json} body with a stable
 * {@code code}, {@code status} and the request's {@code traceId}, and none of SQL, exception class
 * names, stack frames or rejected (PII) values. Pure MockMvc standalone setup: no Spring Boot
 * context, no database.
 */
class ErrorResponseTest {

    static final String PHONE = "+91 98450 12345";
    static final String PHONE_COMPACT = "+919845012345";
    static final String EMAIL = "ravi.kumar@example.com";
    static final String SQL = "SELECT * FROM job WHERE payload = '{\"phone\":\"" + PHONE + "\"}'";
    static final String FAKE_STACK = "java.lang.IllegalStateException: boom\n\tat com.moveinsync.scheduler.app.Foo.bar(Foo.java:42)";
    static final String TRACE_ID = "test-trace-1";

    static final List<String> FORBIDDEN_FRAGMENTS = List.of(SQL, "SELECT", "Exception", "at com.", "\tat ",
            PHONE, PHONE_COMPACT, "98450", EMAIL, "ravi.kumar", "stackTrace", "trace\":", "payload =", "FROM job");

    private final ObjectMapper mapper = new ObjectMapper();
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(new ThrowingController())
                .setControllerAdvice(new ProblemDetailAdvice())
                .build();
        MDC.put("traceId", TRACE_ID);
    }

    @AfterEach
    void clearMdc() {
        MDC.remove("traceId");
    }

    static Stream<Arguments> thrown() {
        return Stream.of(
                Arguments.of("raw", 500, "INTERNAL_ERROR"),
                Arguments.of("bad-sql", 500, "INTERNAL_ERROR"),
                Arguments.of("integrity", 409, "CONCURRENT_MODIFICATION"),
                Arguments.of("no-connection", 503, "SCHEDULER_STORE_UNAVAILABLE"),
                Arguments.of("no-transaction", 503, "SCHEDULER_STORE_UNAVAILABLE"),
                Arguments.of("not-found", 404, "JOB_NOT_FOUND"),
                Arguments.of("validation", 400, "VALIDATION_FAILED"),
                Arguments.of("conflict", 409, "JOB_DISABLED"),
                Arguments.of("store-unavailable", 503, "SCHEDULER_STORE_UNAVAILABLE"),
                Arguments.of("forbidden", 403, "FORBIDDEN"));
    }

    @ParameterizedTest(name = "{0} -> {1} {2}")
    @MethodSource("thrown")
    void exceptionsMapToSafeProblems(String kind, int status, String code) throws Exception {
        JsonNode body = assertSafeProblem(get("/boom/" + kind), status, code);
        if (kind.equals("validation")) {
            assertThat(body.get("errors").get(0).get("field").asText()).isEqualTo("phone");
        }
    }

    @Test
    void beanValidationNamesTheFieldButNeverTheValue() throws Exception {
        String json = "{\"phone\":\"" + PHONE + "\",\"email\":\"" + EMAIL + "\"}";
        JsonNode body = assertSafeProblem(post("/contacts").contentType(MediaType.APPLICATION_JSON).content(json),
                400, "VALIDATION_FAILED");
        List<String> fields = body.get("errors").findValuesAsText("field");
        assertThat(fields).containsExactlyInAnyOrder("phone", "email");
        for (JsonNode e : body.get("errors")) {
            assertThat(e.get("reason").asText()).isNotBlank();
        }
    }

    @Test
    void malformedJsonIsNotEchoed() throws Exception {
        String json = "{\"phone\":\"" + PHONE + "\",\"email\":\"" + EMAIL + "\"";
        JsonNode body = assertSafeProblem(post("/contacts").contentType(MediaType.APPLICATION_JSON).content(json),
                400, "MALFORMED_REQUEST");
        assertThat(body.has("errors")).isFalse();
    }

    @Test
    void wrongJsonTypeNamesFieldOnly() throws Exception {
        String json = "{\"phone\":{\"nested\":\"" + PHONE + "\"},\"email\":\"" + EMAIL + "\"}";
        JsonNode body = assertSafeProblem(post("/contacts").contentType(MediaType.APPLICATION_JSON).content(json),
                400, "VALIDATION_FAILED");
        assertThat(body.get("errors").get(0).get("field").asText()).isEqualTo("phone");
    }

    @Test
    void pathVariableTypeMismatchDoesNotEchoTheValue() throws Exception {
        for (String value : List.of(EMAIL, PHONE_COMPACT)) {
            JsonNode body = assertSafeProblem(get("/items/" + value), 400, "VALIDATION_FAILED");
            assertThat(body.get("errors").get(0).get("field").asText()).isEqualTo("id");
        }
    }

    @Test
    void traceIdIsOmittedWhenThereIsNone() throws Exception {
        MDC.remove("traceId");
        MvcResult r = mvc.perform(get("/boom/not-found")).andReturn();
        JsonNode body = mapper.readTree(r.getResponse().getContentAsString());
        assertThat(body.get("code").asText()).isEqualTo("JOB_NOT_FOUND");
        assertThat(body.has("traceId")).isFalse();
    }

    // ------------------------------------------------------------------ helpers

    private JsonNode assertSafeProblem(RequestBuilder request, int status, String code) throws Exception {
        MvcResult r = mvc.perform(request).andReturn();
        String raw = r.getResponse().getContentAsString();
        assertThat(r.getResponse().getStatus()).as(raw).isEqualTo(status);
        assertThat(r.getResponse().getContentType()).as(raw).startsWith(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        JsonNode body = mapper.readTree(raw);
        assertThat(body.get("code").asText()).as(raw).isEqualTo(code);
        assertThat(body.get("status").asInt()).as(raw).isEqualTo(status);
        assertThat(body.get("detail").asText()).as(raw).isNotBlank();
        assertThat(body.get("traceId").asText()).as(raw).isEqualTo(TRACE_ID);
        for (String fragment : FORBIDDEN_FRAGMENTS) {
            assertThat(raw).as("response must not contain [%s]", fragment).doesNotContain(fragment);
        }
        return body;
    }

    /** A body whose values look like PII and violate their constraints. */
    record ContactRequest(@Size(max = 5) String phone, @Pattern(regexp = "[a-z]+") String email) {
    }

    @RestController
    static class ThrowingController {

        @GetMapping("/boom/{kind}")
        String boom(@PathVariable String kind) {
            SQLException sqlCause = new SQLException(SQL, "42601");
            throw switch (kind) {
                case "raw" -> new RuntimeException("query failed: " + SQL + "\n" + FAKE_STACK);
                case "bad-sql" -> new BadSqlGrammarException("job lookup", SQL, sqlCause);
                case "integrity" -> new DataIntegrityViolationException("PreparedStatementCallback; " + SQL
                        + "; ERROR: duplicate key value violates unique constraint", sqlCause);
                case "no-connection" -> new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection: "
                        + SQL, sqlCause);
                case "no-transaction" -> new CannotCreateTransactionException("Could not open JDBC Connection for "
                        + SQL, sqlCause);
                case "not-found" -> NotFoundException.job();
                case "validation" -> ValidationException.field("phone", "must be a valid phone number");
                case "conflict" -> new ConflictException(ErrorCodes.JOB_DISABLED, "Job is disabled");
                case "store-unavailable" -> new StoreUnavailableException(sqlCause);
                case "forbidden" -> new ForbiddenException("Your role may not perform this action");
                default -> new IllegalArgumentException(kind);
            };
        }

        @PostMapping("/contacts")
        String contacts(@Valid @RequestBody ContactRequest request) {
            return "ok";
        }

        @GetMapping("/items/{id}")
        String item(@PathVariable UUID id) {
            return id.toString();
        }
    }
}
