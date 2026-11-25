package com.asyncjobs.store;

import com.asyncjobs.domain.Job;
import com.asyncjobs.domain.NewJob;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Base for integration tests that drive the store directly against a real Postgres, with the background
 * worker switched off so every state transition is under the test's control.
 *
 * <p>Needs a database: see README "Running the tests".
 */
@SpringBootTest(properties = "jobs.worker.enabled=false")
public abstract class AbstractStoreIT {

    protected static final long GRACE_SECONDS = 15;

    @Autowired
    protected JobStore store;
    @Autowired
    protected JdbcTemplate jdbc;
    @Autowired
    protected ObjectMapper json;

    @BeforeEach
    public void cleanDatabase() {
        jdbc.execute("TRUNCATE jobs, dead_letters");
        jdbc.update("UPDATE queue_control SET paused = false WHERE id = 1");
    }

    protected Job enqueue(String type, int priority, int maxRetries) {
        return store.enqueue(new NewJob(type, json.createObjectNode().put("n", 1), priority, maxRetries, 30, null, 0,
                null)).job();
    }

    protected Job reload(Job job) {
        return store.find(job.id()).orElseThrow();
    }

    /** Makes a job's back-off or delay elapse without sleeping. */
    protected void makeDue(Job job) {
        jdbc.update("UPDATE jobs SET run_at = now() - interval '1 second' WHERE id = ?", job.id());
    }

    /** Simulates the worker holding this job having died: its lease runs out. */
    protected void expireLease(Job job) {
        jdbc.update("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = ?", job.id());
    }
}
