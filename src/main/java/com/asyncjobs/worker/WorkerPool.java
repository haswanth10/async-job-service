package com.asyncjobs.worker;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.asyncjobs.config.JobsProperties;
import com.asyncjobs.domain.Job;
import com.asyncjobs.domain.JobState;
import com.asyncjobs.metrics.JobMetrics;
import com.asyncjobs.store.JobStore;
import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Pulls jobs from the queue and runs them.
 *
 * <pre>
 *  dispatcher thread ──claim(free slots)──▶ slot (virtual thread) ──▶ handler (virtual thread)
 *        ▲                                        │  waits at most timeoutSeconds
 *        └──────────── permit released ◀──────────┘  then records success / failure
 *  reaper thread: every reap-interval, requeues or dead-letters jobs whose lease expired
 * </pre>
 *
 * <p>Concurrency is bounded by a semaphore, and the dispatcher only claims as many jobs as it has free
 * slots, so a worker never holds leases on work it is not actually executing.
 */
@Component
@ConditionalOnProperty(prefix = "jobs.worker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class WorkerPool implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);
    private static final int MAX_ERROR_LENGTH = 4000;

    private final JobStore store;
    private final HandlerRegistry registry;
    private final JobMetrics metrics;
    private final JobsProperties.Worker config;
    private final BackoffPolicy backoff;
    private final String workerId;

    private final Semaphore slots;
    private final Map<UUID, Thread> inFlight = new ConcurrentHashMap<>();
    private volatile boolean running;
    private volatile boolean shuttingDown;
    private Thread dispatcher;
    private ExecutorService slotExecutor;
    private ExecutorService handlerExecutor;
    private ScheduledExecutorService reaper;

    public WorkerPool(JobStore store, HandlerRegistry registry, JobMetrics metrics, JobsProperties properties) {
        this.store = store;
        this.registry = registry;
        this.metrics = metrics;
        this.config = properties.worker();
        this.backoff = new BackoffPolicy(config.backoff().base(), config.backoff().max(), config.backoff().jitter());
        this.workerId = ManagementFactory.getRuntimeMXBean().getName() + "/" + UUID.randomUUID().toString().substring(0, 8);
        this.slots = new Semaphore(config.concurrency());
        metrics.registerWorker(this::busy, config.concurrency());
    }

    public String workerId() {
        return workerId;
    }

    public int busy() {
        return config.concurrency() - slots.availablePermits();
    }

    // ------------------------------------------------------------------ lifecycle

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        shuttingDown = false;
        slotExecutor = Executors.newVirtualThreadPerTaskExecutor();
        handlerExecutor = Executors.newVirtualThreadPerTaskExecutor();
        reaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "job-reaper");
            t.setDaemon(true);
            return t;
        });
        long reapMillis = Math.max(100, config.reapInterval().toMillis());
        reaper.scheduleWithFixedDelay(this::reap, reapMillis, reapMillis, TimeUnit.MILLISECONDS);
        dispatcher = new Thread(this::dispatchLoop, "job-dispatcher");
        dispatcher.setDaemon(true);
        dispatcher.start();
        log.info("Worker {} started: concurrency={}, handlers={}", workerId, config.concurrency(), registry.types());
    }

    /**
     * Graceful shutdown: stop claiming, give in-flight jobs {@code shutdown-grace} to finish, then interrupt
     * the stragglers and release them back to the queue without consuming an attempt.
     */
    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        dispatcher.interrupt();
        reaper.shutdownNow();
        try {
            dispatcher.join(2000);
            boolean drained = slots.tryAcquire(config.concurrency(), config.shutdownGrace().toMillis(), TimeUnit.MILLISECONDS);
            if (!drained) {
                log.warn("Worker {} shutting down with {} job(s) still running; releasing them", workerId, inFlight.size());
                shuttingDown = true;
                inFlight.values().forEach(Thread::interrupt);
                slots.tryAcquire(config.concurrency(), 5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        slotExecutor.shutdownNow();
        handlerExecutor.shutdownNow();
        slots.drainPermits();
        slots.release(config.concurrency());
        log.info("Worker {} stopped", workerId);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Stop before the web server and the connection pool go away. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    // ------------------------------------------------------------------ dispatch

    private void dispatchLoop() {
        long leaseGraceSeconds = Math.max(1, config.leaseGrace().toSeconds());
        while (running) {
            int permits = 0;
            try {
                // Block until at least one slot is free, then take every free slot.
                if (!slots.tryAcquire(config.pollInterval().toMillis(), TimeUnit.MILLISECONDS)) {
                    continue;
                }
                permits = 1 + slots.drainPermits();
                List<Job> claimed = store.claim(workerId, permits, leaseGraceSeconds);
                slots.release(permits - claimed.size());
                permits = claimed.size();
                for (Job job : claimed) {
                    slotExecutor.execute(() -> runAndRelease(job));
                    permits--;
                }
                if (claimed.isEmpty()) {
                    Thread.sleep(config.pollInterval().toMillis());
                }
            } catch (InterruptedException e) {
                slots.release(permits);
                return;
            } catch (RuntimeException e) {
                slots.release(permits);
                log.error("Dispatcher error (will retry): {}", e.toString());
                sleepQuietly(1000);
            }
        }
    }

    private void runAndRelease(Job job) {
        inFlight.put(job.id(), Thread.currentThread()); // put the in-flight job status on concurrent hashmap.
        try {
            execute(job);
        } catch (RuntimeException e) {
            // Could not record the outcome (e.g. database down). The lease will expire and the job is redelivered.
            log.error("Job {} outcome could not be recorded; it will be redelivered after its lease expires: {}",
                    job.id(), e.toString());
        } finally {
            inFlight.remove(job.id());
            slots.release();
        }
    }

    /** Runs one attempt of one job and records the outcome. Package-private for tests. */
    void execute(Job job) {
        metrics.queueWait(job.type(), Duration.between(job.runAt(), job.startedAt()));
        Optional<JobHandler> handler = registry.find(job.type());
        if (handler.isEmpty()) {
            recordFailure(job, "No handler registered for job type '" + job.type() + "'", false, "failed", Duration.ZERO);
            return;
        }
        JobContext context = new JobContext(job.id(), job.type(), job.payload(), job.attempts(), job.maxRetries());
        long startNanos = System.nanoTime();
        Future<JsonNode> future = handlerExecutor.submit(() -> handler.get().handle(context));
        try {
            JsonNode result = future.get(job.timeoutSeconds(), TimeUnit.SECONDS);
            Duration took = Duration.ofNanos(System.nanoTime() - startNanos);
            if (store.succeed(job.id(), workerId, job.attempts(), result)) {
                metrics.attempt(job.type(), "succeeded", took);
                metrics.endToEnd(job.type(), Duration.between(job.createdAt(), job.startedAt()).plus(took));
                log.info("Job {} succeeded: type={} attempt={} durationMs={}", job.id(), job.type(), job.attempts(), took.toMillis());
            } else {
                stale(job, took);
            }
        } catch (TimeoutException e) {
            future.cancel(true);
            recordFailure(job, "Attempt " + job.attempts() + " timed out after " + job.timeoutSeconds() + "s",
                    true, "timeout", Duration.ofNanos(System.nanoTime() - startNanos));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            boolean retryable = !(cause instanceof NonRetryableJobException);
            recordFailure(job, describe(cause), retryable, "failed", Duration.ofNanos(System.nanoTime() - startNanos));
        } catch (InterruptedException e) {
            // Only happens on shutdown. Hand the job back so another worker picks it up straight away.
            future.cancel(true);
            if (shuttingDown && store.release(job.id(), workerId, job.attempts())) {
                log.info("Job {} released back to the queue on shutdown", job.id());
            }
        }
    }

    private void recordFailure(Job job, String error, boolean retryable, String outcome, Duration took) {
        long delay = backoff.delayMillis(job.attempts());
        Optional<Job> updated = store.fail(job.id(), workerId, job.attempts(), truncate(error), retryable, delay);
        if (updated.isEmpty()) {
            stale(job, took);
            return;
        }
        if (updated.get().state() == JobState.DEAD_LETTERED) {
            String reason = retryable ? "retries_exhausted" : "non_retryable";
            metrics.attempt(job.type(), "dead_lettered", took);
            metrics.deadLettered(job.type(), reason);
            log.warn("Job {} dead-lettered: type={} attempts={} reason={} error={}", job.id(), job.type(), job.attempts(), reason, error);
        } else {
            metrics.attempt(job.type(), outcome, took);
            log.info("Job {} attempt {} failed, retrying in ~{}ms: {}", job.id(), job.attempts(), delay, error);
        }
    }

    private void stale(Job job, Duration took) {
        metrics.attempt(job.type(), "stale", took);
        metrics.staleCompletion(job.type());
        log.warn("Job {} attempt {} finished after its lease was lost; outcome discarded", job.id(), job.attempts());
    }

    // ------------------------------------------------------------------ reaper

    /** Reclaims expired leases. Safe to run on every worker concurrently (SKIP LOCKED). */
    public int reap() {
        try {
            List<Job> reaped = store.reapExpiredLeases(config.backoff().base().toMillis(), config.backoff().max().toMillis(), 100);
            for (Job job : reaped) {
                if (job.state() == JobState.DEAD_LETTERED) {
                    metrics.deadLettered(job.type(), "lease_expired");
                    log.warn("Job {} dead-lettered after lease expiry: type={} attempts={}", job.id(), job.type(), job.attempts());
                } else {
                    log.warn("Job {} lease expired; requeued for attempt {}", job.id(), job.attempts() + 1);
                }
            }
            return reaped.size();
        } catch (RuntimeException e) {
            log.error("Reaper error (will retry): {}", e.toString());
            return 0;
        }
    }

    // ------------------------------------------------------------------ helpers

    private static String describe(Throwable t) {
        String message = t.getMessage();
        return t.getClass().getSimpleName() + (message == null || message.isBlank() ? "" : ": " + message);
    }

    private static String truncate(String s) {
        return s.length() <= MAX_ERROR_LENGTH ? s : s.substring(0, MAX_ERROR_LENGTH) + "…";
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
