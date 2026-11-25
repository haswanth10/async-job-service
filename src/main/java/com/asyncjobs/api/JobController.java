package com.asyncjobs.api;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import com.asyncjobs.config.JobsProperties;
import com.asyncjobs.domain.Job;
import com.asyncjobs.domain.JobState;
import com.asyncjobs.domain.NewJob;
import com.asyncjobs.metrics.JobMetrics;
import com.asyncjobs.store.JobStore;
import com.asyncjobs.worker.HandlerRegistry;

import jakarta.validation.Valid;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/v1/jobs")
@ConditionalOnProperty(prefix = "jobs.api", name = "enabled", havingValue = "true", matchIfMissing = true)
public class JobController {

    private final JobStore store;
    private final HandlerRegistry registry;
    private final JobMetrics metrics;
    private final JobsProperties properties;

    public JobController(JobStore store, HandlerRegistry registry, JobMetrics metrics, JobsProperties properties) {
        this.store = store;
        this.registry = registry;
        this.metrics = metrics;
        this.properties = properties;
    }

    /**
     * Enqueues a job and returns immediately: 202 with the job (including its id) and a Location header.
     * The only work done on the request thread is one INSERT; execution happens on a worker.
     *
     * <p>With an {@code Idempotency-Key} header, a repeated submission returns the original job with 200
     * instead of creating a second one.
     */
    @PostMapping
    public ResponseEntity<Job> submit(@Valid @RequestBody SubmitJobRequest request,
                                      @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        if (!registry.supports(request.type())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Unknown job type '" + request.type() + "'. Known types: " + registry.types());
        }
        if (idempotencyKey != null && (idempotencyKey.isBlank() || idempotencyKey.length() > 200)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Idempotency-Key must be 1..200 characters");
        }
        if (request.payload() != null && request.payload().toString().length() > properties.api().maxPayloadBytes()) {
            throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE,
                    "Payload exceeds " + properties.api().maxPayloadBytes() + " bytes; store large inputs elsewhere and pass a reference");
        }
        long maxDepth = properties.api().maxQueueDepth();
        if (maxDepth > 0 && metrics.depth().pending() >= maxDepth) {
            // Back-pressure: shed load at the door rather than let the queue grow without bound.
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).header(HttpHeaders.RETRY_AFTER, "5").build();
        }

        JobsProperties.Defaults defaults = properties.defaults();
        JobStore.Enqueued enqueued = store.enqueue(new NewJob(
                request.type(),
                request.payload(),
                request.priority() != null ? request.priority() : defaults.priority(),
                request.maxRetries() != null ? request.maxRetries() : defaults.maxRetries(),
                request.timeoutSeconds() != null ? request.timeoutSeconds() : defaults.timeoutSeconds(),
                request.runAt(),
                request.delaySeconds() != null ? request.delaySeconds() * 1000 : 0,
                idempotencyKey));
        if (enqueued.created()) {
            metrics.submitted(request.type());
        }
        Job job = enqueued.job();
        return ResponseEntity.status(enqueued.created() ? HttpStatus.ACCEPTED : HttpStatus.OK)
                .location(URI.create("/api/v1/jobs/" + job.id()))
                .body(job);
    }

    /** Current state, attempt count, last error and result for one job. */
    @GetMapping("/{id}")
    public Job get(@PathVariable UUID id) {
        return store.find(id).orElseThrow(() -> notFound(id));
    }

    @GetMapping
    public List<Job> list(@RequestParam(required = false) String state,
                          @RequestParam(defaultValue = "50") int limit) {
        JobState filter = null;
        if (state != null) {
            try {
                filter = JobState.from(state);
            } catch (IllegalArgumentException e) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
            }
        }
        return store.list(filter, Math.max(1, Math.min(limit, 500)));
    }

    /** Cancels a job that has not started its next attempt. 409 if it is running or already finished. */
    @DeleteMapping("/{id}")
    public Job cancel(@PathVariable UUID id) {
        return switch (store.cancel(id)) {
            case CANCELLED -> store.find(id).orElseThrow(() -> notFound(id));
            case NOT_FOUND -> throw notFound(id);
            case NOT_CANCELLABLE -> throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Job " + id + " is running or already finished and cannot be cancelled");
        };
    }

    @PostMapping("/{id}/cancel")
    public Job cancelViaPost(@PathVariable UUID id) {
        return cancel(id);
    }

    private static ResponseStatusException notFound(UUID id) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "No job with id " + id);
    }
}
