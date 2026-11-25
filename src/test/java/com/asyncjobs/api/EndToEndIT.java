package com.asyncjobs.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Full-stack tests: real HTTP server, real Postgres, real worker pool and reaper, with intervals
 * shortened so retries and timeouts complete in milliseconds.
 *
 * <p>The context is discarded afterwards so its worker cannot steal jobs from store-level tests.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "jobs.worker.concurrency=4",
        "jobs.worker.poll-interval=50ms",
        "jobs.worker.reap-interval=200ms",
        "jobs.worker.lease-grace=1s",
        "jobs.worker.shutdown-grace=5s",
        "jobs.worker.backoff.base=100ms",
        "jobs.worker.backoff.max=500ms",
        "jobs.worker.backoff.jitter=false"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class EndToEndIT {

    private static final String JOBS = "/api/v1/jobs";

    @Autowired
    TestRestTemplate http;
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    MeterRegistry meters;

    @BeforeEach
    void cleanDatabase() {
        jdbc.execute("TRUNCATE jobs, dead_letters");
        jdbc.update("UPDATE queue_control SET paused = false WHERE id = 1");
    }

    // ------------------------------------------------------------------ submission and status

    @Test
    void submissionReturnsAnIdImmediatelyAndTheJobRunsInTheBackground() {
        ResponseEntity<JsonNode> response = submit(Map.of("type", "echo", "payload", Map.of("msg", "hi")));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        JsonNode accepted = response.getBody();
        String id = accepted.path("id").asText();
        assertThat(UUID.fromString(id)).isNotNull();
        assertThat(accepted.path("state").asText()).isEqualTo("queued");
        assertThat(accepted.path("attempts").asInt()).isZero();
        assertThat(response.getHeaders().getLocation().toString()).endsWith(JOBS + "/" + id);

        JsonNode done = awaitState(id, "succeeded");
        assertThat(done.path("attempts").asInt()).isEqualTo(1);
        assertThat(done.path("result").path("echo").path("msg").asText()).isEqualTo("hi");
        assertThat(absent(done.path("lastError"))).isTrue();
        assertThat(absent(done.path("finishedAt"))).isFalse();
    }

    @Test
    void omittedSchedulingParametersFallBackToDefaults() {
        JsonNode job = submit(Map.of("type", "echo")).getBody();

        assertThat(job.path("priority").asInt()).isEqualTo(0);
        assertThat(job.path("maxRetries").asInt()).isEqualTo(3);
        assertThat(job.path("timeoutSeconds").asInt()).isEqualTo(30);
    }

    @Test
    void invalidSubmissionsAreRejected() {
        assertThat(submit(Map.of()).getStatusCode()).as("missing type").isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(submit(Map.of("type", "no-such-handler")).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(submit(Map.of("type", "echo", "priority", 101)).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(submit(Map.of("type", "echo", "maxRetries", -1)).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(submit(Map.of("type", "echo", "timeoutSeconds", 0)).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(submit(Map.of("type", "echo", "delaySeconds", -5)).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> malformed = http.postForEntity(JOBS, new HttpEntity<>("{not json", headers), String.class);
        assertThat(malformed.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs", Long.class)).isZero();
    }

    @Test
    void unknownJobIdIs404AndMalformedIdIs400() {
        assertThat(http.getForEntity(JOBS + "/" + UUID.randomUUID(), JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(http.getForEntity(JOBS + "/not-a-uuid", String.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void repeatedSubmissionWithTheSameIdempotencyKeyReturnsTheOriginalJob() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", "payment-" + UUID.randomUUID());
        Map<String, Object> body = Map.of("type", "echo", "delaySeconds", 3600);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);

        ResponseEntity<JsonNode> first = http.postForEntity(JOBS, request, JsonNode.class);
        ResponseEntity<JsonNode> second = http.postForEntity(JOBS, request, JsonNode.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().path("id").asText()).isEqualTo(first.getBody().path("id").asText());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs", Long.class)).isEqualTo(1);
    }

    @Test
    void jobsCanBeListedByState() {
        String id = submit(Map.of("type", "echo", "delaySeconds", 3600)).getBody().path("id").asText();

        JsonNode queued = http.getForObject(JOBS + "?state=queued", JsonNode.class);
        assertThat(queued).hasSize(1);
        assertThat(queued.get(0).path("id").asText()).isEqualTo(id);
        assertThat(http.getForObject(JOBS + "?state=succeeded", JsonNode.class)).isEmpty();
        assertThat(http.getForEntity(JOBS + "?state=bogus", String.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    // ------------------------------------------------------------------ retries, timeouts, dead letters

    @Test
    void transientFailuresAreRetriedUntilTheJobSucceeds() {
        String id = submit(Map.of("type", "flaky", "payload", Map.of("failTimes", 2), "maxRetries", 3))
                .getBody().path("id").asText();

        JsonNode done = awaitState(id, "succeeded");

        assertThat(done.path("attempts").asInt()).isEqualTo(3);
        assertThat(done.path("result").path("succeededOnAttempt").asInt()).isEqualTo(3);
    }

    @Test
    void jobThatExhaustsItsRetriesIsDeadLetteredAndCanBeRequeued() {
        String id = submit(Map.of("type", "flaky", "payload", Map.of("failTimes", 99), "maxRetries", 2))
                .getBody().path("id").asText();

        JsonNode dead = awaitState(id, "dead_lettered");

        assertThat(dead.path("attempts").asInt()).as("1 attempt + 2 retries").isEqualTo(3);
        assertThat(dead.path("lastError").asText()).contains("simulated transient failure on attempt 3");
        assertThat(absent(dead.path("result"))).isTrue();

        JsonNode letters = http.getForObject("/api/v1/dead-letters", JsonNode.class);
        assertThat(letters).hasSize(1);
        assertThat(letters.get(0).path("jobId").asText()).isEqualTo(id);
        assertThat(letters.get(0).path("reason").asText()).isEqualTo("retries_exhausted");
        assertThat(letters.get(0).path("attempts").asInt()).isEqualTo(3);
        assertThat(depth().path("deadLettered").asLong()).isEqualTo(1);
        // The worker records metrics just after committing the outcome, so allow it a moment.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(meters.find("jobs.dead.lettered").counters().stream().mapToDouble(Counter::count).sum())
                        .isGreaterThanOrEqualTo(1));

        ResponseEntity<JsonNode> requeued = http.postForEntity("/api/v1/dead-letters/" + id + "/requeue", null, JsonNode.class);
        assertThat(requeued.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(requeued.getBody().path("state").asText()).isEqualTo("queued");
        assertThat(requeued.getBody().path("attempts").asInt()).isZero();

        assertThat(http.postForEntity("/api/v1/dead-letters/" + UUID.randomUUID() + "/requeue", null, String.class)
                .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void attemptThatExceedsItsTimeoutIsFailedAndRetried() {
        String id = submit(Map.of("type", "sleep", "payload", Map.of("ms", 20_000), "timeoutSeconds", 1, "maxRetries", 1))
                .getBody().path("id").asText();

        JsonNode dead = awaitState(id, "dead_lettered");

        assertThat(dead.path("attempts").asInt()).isEqualTo(2);
        assertThat(dead.path("lastError").asText()).contains("timed out after 1s");
    }

    @Test
    void nonRetryableFailureSkipsTheRemainingRetries() {
        String id = submit(Map.of("type", "flaky", "payload", Map.of("permanent", true), "maxRetries", 5))
                .getBody().path("id").asText();

        JsonNode dead = awaitState(id, "dead_lettered");

        assertThat(dead.path("attempts").asInt()).isEqualTo(1);
        assertThat(dead.path("lastError").asText()).contains("permanent failure");
        assertThat(http.getForObject("/api/v1/dead-letters", JsonNode.class).get(0).path("reason").asText())
                .isEqualTo("non_retryable");
    }

    @Test
    void jobHeldByACrashedWorkerIsRedeliveredAndCompleted() {
        post("/api/v1/queue/pause");
        String id = submit(Map.of("type", "echo", "maxRetries", 3)).getBody().path("id").asText();
        // A worker claimed the job and then died: the row is 'running' under a lease that has run out.
        jdbc.update("""
                UPDATE jobs SET state = 'running', attempts = 1, locked_by = 'ghost-worker', started_at = now(),
                                lease_expires_at = now() - interval '1 second'
                 WHERE id = ?""", UUID.fromString(id));

        post("/api/v1/queue/resume");
        JsonNode done = awaitState(id, "succeeded");

        assertThat(done.path("attempts").asInt()).as("the lost attempt is counted").isEqualTo(2);
        assertThat(done.path("result").path("attempt").asInt()).isEqualTo(2);
    }

    // ------------------------------------------------------------------ delay, cancel, pause, drain, depth

    @Test
    void delayedJobWaitsAndCanBeCancelled() throws Exception {
        String delayed = submit(Map.of("type", "echo", "delaySeconds", 3600)).getBody().path("id").asText();
        String scheduled = submit(Map.of("type", "echo", "runAt", Instant.now().plusSeconds(3600).toString()))
                .getBody().path("id").asText();

        Thread.sleep(400); // several poll intervals
        assertThat(get(delayed).path("state").asText()).isEqualTo("queued");
        assertThat(get(scheduled).path("state").asText()).isEqualTo("queued");
        assertThat(depth().path("delayed").asLong()).isEqualTo(2);
        assertThat(depth().path("ready").asLong()).isZero();

        ResponseEntity<JsonNode> cancelled = http.exchange(JOBS + "/" + delayed, HttpMethod.DELETE, null, JsonNode.class);
        assertThat(cancelled.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(cancelled.getBody().path("state").asText()).isEqualTo("cancelled");

        assertThat(http.exchange(JOBS + "/" + delayed, HttpMethod.DELETE, null, String.class).getStatusCode())
                .as("already cancelled").isEqualTo(HttpStatus.CONFLICT);
        assertThat(http.exchange(JOBS + "/" + UUID.randomUUID(), HttpMethod.DELETE, null, String.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(http.postForEntity(JOBS + "/" + scheduled + "/cancel", null, JsonNode.class).getBody()
                .path("state").asText()).isEqualTo("cancelled");
    }

    @Test
    void finishedJobCannotBeCancelled() {
        String id = submit(Map.of("type", "echo")).getBody().path("id").asText();
        awaitState(id, "succeeded");

        assertThat(http.exchange(JOBS + "/" + id, HttpMethod.DELETE, null, String.class).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void pausedQueueAcceptsWorkButRunsNothingAndDrainCancelsIt() throws Exception {
        assertThat(post("/api/v1/queue/pause").path("paused").asBoolean()).isTrue();

        String a = submit(Map.of("type", "echo")).getBody().path("id").asText();
        String b = submit(Map.of("type", "echo")).getBody().path("id").asText();
        String c = submit(Map.of("type", "sleep")).getBody().path("id").asText();
        Thread.sleep(400);

        JsonNode depth = depth();
        assertThat(depth.path("paused").asBoolean()).isTrue();
        assertThat(depth.path("ready").asLong()).isEqualTo(3);
        assertThat(depth.path("pending").asLong()).isEqualTo(3);
        assertThat(depth.path("running").asLong()).isZero();

        assertThat(post("/api/v1/queue/drain?type=sleep").path("cancelled").asInt()).isEqualTo(1);
        assertThat(get(c).path("state").asText()).isEqualTo("cancelled");
        assertThat(post("/api/v1/queue/drain").path("cancelled").asInt()).isEqualTo(2);
        assertThat(get(a).path("state").asText()).isEqualTo("cancelled");
        assertThat(get(b).path("state").asText()).isEqualTo("cancelled");
        assertThat(depth().path("pending").asLong()).isZero();

        assertThat(post("/api/v1/queue/resume").path("paused").asBoolean()).isFalse();
        String after = submit(Map.of("type", "echo")).getBody().path("id").asText();
        awaitState(after, "succeeded");
    }

    // ------------------------------------------------------------------ observability

    @Test
    void metricsAreRecordedForCompletedJobs() {
        String id = submit(Map.of("type", "echo")).getBody().path("id").asText();
        awaitState(id, "succeeded");

        // The worker records metrics just after committing the outcome, so allow it a moment.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(meters.get("jobs.submitted").tag("type", "echo").counter().count()).isGreaterThanOrEqualTo(1);
            assertThat(meters.get("jobs.execution.duration").tag("type", "echo").tag("outcome", "succeeded").timer().count())
                    .isGreaterThanOrEqualTo(1);
            assertThat(meters.get("jobs.queue.wait").tag("type", "echo").timer().count()).isGreaterThanOrEqualTo(1);
            assertThat(meters.get("jobs.latency").tag("type", "echo").timer().count()).isGreaterThanOrEqualTo(1);
        });
        assertThat(meters.get("jobs.worker.concurrency").gauge().value()).isEqualTo(4.0);
        assertThat(meters.get("jobs.worker.utilization").gauge().value()).isBetween(0.0, 1.0);
        assertThat(meters.get("jobs.queue.depth").tag("state", "ready").gauge().value()).isGreaterThanOrEqualTo(0.0);
        assertThat(meters.get("jobs.queue.depth").tag("state", "dead_lettered").gauge().value()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void healthEndpointReportsUp() {
        JsonNode health = http.getForObject("/actuator/health", JsonNode.class);

        assertThat(health.path("status").asText()).isEqualTo("UP");
    }

    // ------------------------------------------------------------------ helpers

    private ResponseEntity<JsonNode> submit(Map<String, Object> body) {
        return http.postForEntity(JOBS, body, JsonNode.class);
    }

    private JsonNode get(String id) {
        return http.getForObject(JOBS + "/" + id, JsonNode.class);
    }

    private JsonNode post(String path) {
        return http.postForObject(path, null, JsonNode.class);
    }

    private JsonNode depth() {
        return http.getForObject("/api/v1/queue/depth", JsonNode.class);
    }

    /** True for JSON null or a field that is not present at all. */
    private static boolean absent(JsonNode node) {
        return node.isNull() || node.isMissingNode();
    }

    private JsonNode awaitState(String id, String expected) {
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(50)).untilAsserted(() ->
                assertThat(get(id).path("state").asText()).isEqualTo(expected));
        return get(id);
    }
}
