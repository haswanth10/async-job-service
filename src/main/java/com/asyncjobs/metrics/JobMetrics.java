package com.asyncjobs.metrics;

import java.time.Duration;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

import com.asyncjobs.domain.QueueDepth;
import com.asyncjobs.store.JobStore;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * All job metrics in one place. Exposed at {@code /actuator/prometheus} and {@code /actuator/metrics}.
 *
 * <ul>
 *   <li>{@code jobs.queue.depth{state}} – gauge: ready, delayed, retrying, running, dead_lettered</li>
 *   <li>{@code jobs.queue.oldest.ready.age} – gauge (seconds)</li>
 *   <li>{@code jobs.worker.busy}, {@code jobs.worker.concurrency}, {@code jobs.worker.utilization} – gauges</li>
 *   <li>{@code jobs.execution.duration{type,outcome}} – timer with p50/p95: handler run time per attempt</li>
 *   <li>{@code jobs.queue.wait{type}} – timer with p50/p95: due-time to claim</li>
 *   <li>{@code jobs.latency{type}} – timer with p50/p95: submission to success, end to end</li>
 *   <li>{@code jobs.submitted}, {@code jobs.attempts{type,outcome}}, {@code jobs.dead.lettered{type,reason}},
 *       {@code jobs.stale.completions} – counters</li>
 * </ul>
 */
@Component
public class JobMetrics {

    private static final Logger log = LoggerFactory.getLogger(JobMetrics.class);
    private static final long DEPTH_CACHE_NANOS = Duration.ofSeconds(2).toNanos();

    private final MeterRegistry registry;
    private final JobStore store;

    private volatile QueueDepth cachedDepth = new QueueDepth(0, 0, 0, 0, 0, 0, false);
    private volatile long cachedAt;
    private volatile boolean loaded;

    public JobMetrics(MeterRegistry registry, JobStore store) {
        this.registry = registry;
        this.store = store;
        depthGauge("ready", d -> d.ready());
        depthGauge("delayed", d -> d.delayed());
        depthGauge("retrying", d -> d.retrying());
        depthGauge("running", d -> d.running());
        depthGauge("dead_lettered", d -> d.deadLettered());
        Gauge.builder("jobs.queue.oldest.ready.age", this, m -> m.depth().oldestReadyAgeSeconds())
                .baseUnit("seconds")
                .description("Age of the oldest claimable job; rises when workers cannot keep up")
                .register(registry);
    }

    private void depthGauge(String state, ToDoubleFunction<QueueDepth> value) {
        Gauge.builder("jobs.queue.depth", this, m -> value.applyAsDouble(m.depth()))
                .tag("state", state)
                .description("Number of jobs per queue state")
                .register(registry);
    }

    /** Queue depth, cached briefly so scrapes and back-pressure checks do not each hit the database. */
    public QueueDepth depth() {
        long now = System.nanoTime();
        if (!loaded || now - cachedAt > DEPTH_CACHE_NANOS) {
            try {
                cachedDepth = store.depth();
                loaded = true;
            } catch (RuntimeException e) {
                log.warn("Could not refresh queue depth: {}", e.toString());
            }
            cachedAt = now;
        }
        return cachedDepth;
    }

    public void registerWorker(Supplier<Number> busy, int concurrency) {
        Gauge.builder("jobs.worker.busy", busy).description("Jobs executing in this process").register(registry);
        Gauge.builder("jobs.worker.concurrency", () -> concurrency).register(registry);
        Gauge.builder("jobs.worker.utilization", () -> busy.get().doubleValue() / concurrency)
                .description("busy / concurrency, 0..1").register(registry);
    }

    public void submitted(String type) {
        Counter.builder("jobs.submitted").tag("type", type).register(registry).increment();
    }

    public void queueWait(String type, Duration wait) {
        timer("jobs.queue.wait", "Time from a job becoming due to a worker claiming it")
                .tag("type", type).register(registry).record(wait.isNegative() ? Duration.ZERO : wait);
    }

    /** @param outcome succeeded | failed | timeout | dead_lettered | stale */
    public void attempt(String type, String outcome, Duration duration) {
        timer("jobs.execution.duration", "Handler run time per attempt")
                .tag("type", type).tag("outcome", outcome).register(registry).record(duration);
        Counter.builder("jobs.attempts").tag("type", type).tag("outcome", outcome).register(registry).increment();
    }

    public void endToEnd(String type, Duration latency) {
        timer("jobs.latency", "Time from submission to successful completion")
                .tag("type", type).register(registry).record(latency.isNegative() ? Duration.ZERO : latency);
    }

    public void deadLettered(String type, String reason) {
        Counter.builder("jobs.dead.lettered")
                .description("Jobs moved to the dead-letter store; rate() of this is the dead-letter rate")
                .tag("type", type).tag("reason", reason).register(registry).increment();
    }

    /** A worker finished an attempt after losing its lease; the outcome was discarded. */
    public void staleCompletion(String type) {
        Counter.builder("jobs.stale.completions").tag("type", type).register(registry).increment();
    }

    private static Timer.Builder timer(String name, String description) {
        return Timer.builder(name).description(description).publishPercentiles(0.5, 0.95);
    }
}
