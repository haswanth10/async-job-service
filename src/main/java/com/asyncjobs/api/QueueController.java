package com.asyncjobs.api;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.asyncjobs.domain.DeadLetter;
import com.asyncjobs.domain.Job;
import com.asyncjobs.domain.QueueDepth;
import com.asyncjobs.store.JobStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/** Visibility and operational control: depth, pause/resume, drain, and the dead-letter store. */
@RestController
@RequestMapping("/api/v1")
@ConditionalOnProperty(prefix = "jobs.api", name = "enabled", havingValue = "true", matchIfMissing = true)
public class QueueController {

    private static final Logger log = LoggerFactory.getLogger(QueueController.class);

    private final JobStore store;

    public QueueController(JobStore store) {
        this.store = store;
    }

    /** Live queue depth, read straight from the database (not the cached metrics value). */
    @GetMapping("/queue/depth")
    public QueueDepth depth() {
        return store.depth();
    }

    /** Cancels every pending job, optionally restricted to one type. Running jobs finish normally. */
    @PostMapping("/queue/drain")
    public Map<String, Object> drain(@RequestParam(required = false) String type) {
        int cancelled = store.drain(type);
        log.warn("Queue drained: cancelled={} type={}", cancelled, type == null ? "*" : type);
        return Map.of("cancelled", cancelled);
    }

    /** Workers stop claiming new jobs; submissions are still accepted and in-flight jobs finish. */
    @PostMapping("/queue/pause")
    public QueueDepth pause() {
        store.setPaused(true);
        log.warn("Queue paused");
        return store.depth();
    }

    @PostMapping("/queue/resume")
    public QueueDepth resume() {
        store.setPaused(false);
        log.warn("Queue resumed");
        return store.depth();
    }

    @GetMapping("/dead-letters")
    public List<DeadLetter> deadLetters(@RequestParam(defaultValue = "50") int limit) {
        return store.deadLetters(Math.max(1, Math.min(limit, 500)));
    }

    /** Puts a dead-lettered job back on the queue with a fresh retry budget. */
    @PostMapping("/dead-letters/{jobId}/requeue")
    public Job requeue(@PathVariable UUID jobId) {
        return store.requeueDeadLetter(jobId).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                "No dead-lettered job with id " + jobId));
    }
}
