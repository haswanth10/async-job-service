package com.asyncjobs.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.asyncjobs.config.JobsProperties;
import com.asyncjobs.domain.Job;
import com.asyncjobs.domain.JobState;
import com.asyncjobs.handlers.EchoHandler;
import com.asyncjobs.handlers.FlakyHandler;
import com.asyncjobs.handlers.SleepHandler;
import com.asyncjobs.metrics.JobMetrics;
import com.asyncjobs.store.JobStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the attempt-execution logic, with the store mocked out. */
class WorkerPoolTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private JobStore store;
    private SimpleMeterRegistry meters;
    private WorkerPool pool;

    @BeforeEach
    void setUp() {
        store = mock(JobStore.class);
        meters = new SimpleMeterRegistry();
        JobsProperties properties = new JobsProperties(
                new JobsProperties.Api(true, 0, 262_144),
                new JobsProperties.Defaults(0, 3, 30),
                new JobsProperties.Worker(true, 2, Duration.ofMillis(50), Duration.ofSeconds(1), Duration.ofSeconds(1),
                        Duration.ofSeconds(1), new JobsProperties.Backoff(Duration.ofMillis(100), Duration.ofSeconds(1), false)));
        HandlerRegistry registry = new HandlerRegistry(List.of(new EchoHandler(), new FlakyHandler(), new SleepHandler()));
        pool = new WorkerPool(store, registry, new JobMetrics(meters, store), properties);
        pool.start(); // the mocked store hands the dispatcher no jobs; we drive execute() directly
    }

    @AfterEach
    void tearDown() {
        pool.stop();
    }

    @Test
    void successIsRecordedWithTheHandlersResult() throws Exception {
        Job job = running("echo", "{\"hello\":\"world\"}", 1, 3, 5);
        when(store.succeed(eq(job.id()), eq(pool.workerId()), eq(1), any())).thenReturn(true);

        pool.execute(job);

        verify(store).succeed(eq(job.id()), eq(pool.workerId()), eq(1),
                eq(JSON.readTree("{\"echo\":{\"hello\":\"world\"},\"attempt\":1}")));
        verify(store, never()).fail(any(), anyString(), anyInt(), anyString(), anyBoolean(), anyLong());
        assertThat(meters.get("jobs.attempts").tag("outcome", "succeeded").counter().count()).isEqualTo(1);
        assertThat(meters.get("jobs.latency").timer().count()).isEqualTo(1);
    }

    @Test
    void handlerExceptionIsRecordedAsRetryableWithExponentialBackoff() throws Exception {
        Job job = running("flaky", "{\"failTimes\":5}", 3, 5, 5);
        when(store.fail(any(), anyString(), anyInt(), anyString(), anyBoolean(), anyLong()))
                .thenReturn(Optional.of(withState(job, JobState.FAILED)));

        pool.execute(job);

        // attempt 3 with base 100ms and no jitter -> 100 * 2^2
        verify(store).fail(eq(job.id()), eq(pool.workerId()), eq(3), contains("simulated transient failure"), eq(true), eq(400L));
        assertThat(meters.get("jobs.attempts").tag("outcome", "failed").counter().count()).isEqualTo(1);
    }

    @Test
    void nonRetryableExceptionSkipsRemainingRetries() throws Exception {
        Job job = running("flaky", "{\"permanent\":true}", 1, 5, 5);
        when(store.fail(any(), anyString(), anyInt(), anyString(), anyBoolean(), anyLong()))
                .thenReturn(Optional.of(withState(job, JobState.DEAD_LETTERED)));

        pool.execute(job);

        verify(store).fail(eq(job.id()), eq(pool.workerId()), eq(1), contains("permanent failure"), eq(false), anyLong());
        assertThat(meters.get("jobs.dead.lettered").tag("reason", "non_retryable").counter().count()).isEqualTo(1);
    }

    @Test
    void attemptExceedingItsTimeoutIsInterruptedAndFailed() throws Exception {
        Job job = running("sleep", "{\"ms\":30000}", 1, 0, 1);
        when(store.fail(any(), anyString(), anyInt(), anyString(), anyBoolean(), anyLong()))
                .thenReturn(Optional.of(withState(job, JobState.DEAD_LETTERED)));

        long start = System.nanoTime();
        pool.execute(job);
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - start).toMillis();

        assertThat(elapsedMillis).isBetween(900L, 5_000L);
        verify(store).fail(eq(job.id()), eq(pool.workerId()), eq(1), contains("timed out after 1s"), eq(true), anyLong());
        verify(store, never()).succeed(any(), anyString(), anyInt(), any());
        assertThat(meters.get("jobs.dead.lettered").tag("reason", "retries_exhausted").counter().count()).isEqualTo(1);
    }

    @Test
    void unknownJobTypeIsDeadLetteredWithoutRetry() throws Exception {
        Job job = running("no-such-type", "{}", 1, 5, 5);
        when(store.fail(any(), anyString(), anyInt(), anyString(), anyBoolean(), anyLong()))
                .thenReturn(Optional.of(withState(job, JobState.DEAD_LETTERED)));

        pool.execute(job);

        verify(store).fail(eq(job.id()), eq(pool.workerId()), eq(1), contains("No handler registered"), eq(false), anyLong());
    }

    @Test
    void outcomeIsDiscardedWhenTheLeaseWasLost() throws Exception {
        Job job = running("echo", "{}", 1, 3, 5);
        when(store.succeed(any(), anyString(), anyInt(), any())).thenReturn(false); // fenced out

        pool.execute(job);

        assertThat(meters.get("jobs.stale.completions").counter().count()).isEqualTo(1);
        assertThat(meters.find("jobs.attempts").tag("outcome", "succeeded").counter()).isNull();
    }

    private static Job running(String type, String payload, int attempt, int maxRetries, int timeoutSeconds) throws Exception {
        Instant now = Instant.now();
        JsonNode node = JSON.readTree(payload);
        return new Job(UUID.randomUUID(), type, node, 0, JobState.RUNNING, attempt, maxRetries, timeoutSeconds,
                now, now.plusSeconds(timeoutSeconds + 1), "someone", null, null, null, now, now, now, null);
    }

    private static Job withState(Job j, JobState state) {
        return new Job(j.id(), j.type(), j.payload(), j.priority(), state, j.attempts(), j.maxRetries(), j.timeoutSeconds(),
                j.runAt(), null, null, "error", null, null, j.createdAt(), j.updatedAt(), j.startedAt(), null);
    }
}
