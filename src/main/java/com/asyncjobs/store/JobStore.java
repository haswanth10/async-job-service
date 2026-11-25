package com.asyncjobs.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.asyncjobs.domain.DeadLetter;
import com.asyncjobs.domain.Job;
import com.asyncjobs.domain.JobState;
import com.asyncjobs.domain.NewJob;
import com.asyncjobs.domain.QueueDepth;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * The queue, the in-flight state and the result store, all in one Postgres table.
 *
 * <p>Design rules that every statement here follows:
 * <ul>
 *   <li>Every state transition is a single atomic SQL statement, so a crash can never leave a job half-moved.</li>
 *   <li>All timestamps come from the database clock ({@code now()}), so API and worker nodes never need
 *       synchronised clocks.</li>
 *   <li>Completions are fenced on {@code (locked_by, attempts)}: only the worker that holds the current lease
 *       can record an outcome. A worker that lost its lease gets {@code false} back and its result is dropped.</li>
 * </ul>
 */
@Repository
public class JobStore {

    public enum CancelResult { CANCELLED, NOT_FOUND, NOT_CANCELLABLE }

    /** Result of an enqueue: {@code created} is false when an idempotency key matched an existing job. */
    public record Enqueued(Job job, boolean created) {
    }

    private static final String PENDING = "state IN ('queued', 'failed')";

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final RowMapper<Job> jobMapper = this::mapJob;

    public JobStore(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    // ------------------------------------------------------------------ submission

    public Enqueued enqueue(NewJob job) {
        UUID id = UUID.randomUUID();
        List<Job> inserted = jdbc.query("""
                INSERT INTO jobs (id, type, payload, priority, max_retries, timeout_seconds, run_at,
                                  idempotency_key)
                VALUES (?, ?, ?::jsonb, ?, ?, ?,
                        COALESCE(?::timestamptz, now()) + (?::bigint * interval '1 millisecond'), ?)
                ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
                RETURNING *
                """, jobMapper,
                id, job.type(), json(job.payload()), job.priority(), job.maxRetries(), job.timeoutSeconds(),
                timestamp(job.runAt()), job.delayMillis(), job.idempotencyKey());
        if (!inserted.isEmpty()) {
            return new Enqueued(inserted.get(0), true);
        }
        Job existing = jdbc.queryForObject("SELECT * FROM jobs WHERE idempotency_key = ?", jobMapper,
                job.idempotencyKey());
        return new Enqueued(existing, false);
    }

    // ------------------------------------------------------------------ reads

    public Optional<Job> find(UUID id) {
        return jdbc.query("SELECT * FROM jobs WHERE id = ?", jobMapper, id).stream().findFirst();
    }

    public List<Job> list(JobState state, int limit) {
        if (state == null) {
            return jdbc.query("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", jobMapper, limit);
        }
        return jdbc.query("SELECT * FROM jobs WHERE state = ? ORDER BY created_at DESC LIMIT ?", jobMapper,
                state.value(), limit);
    }

    public QueueDepth depth() {
        return jdbc.queryForObject("""
                SELECT count(*) FILTER (WHERE state IN ('queued', 'failed') AND run_at <= now())  AS ready,
                       count(*) FILTER (WHERE state = 'queued' AND run_at > now())               AS delayed,
                       count(*) FILTER (WHERE state = 'failed' AND run_at > now())               AS retrying,
                       count(*) FILTER (WHERE state = 'running')                                 AS running,
                       COALESCE(EXTRACT(EPOCH FROM now() - min(run_at)
                           FILTER (WHERE state IN ('queued', 'failed') AND run_at <= now())), 0) AS oldest_ready_age,
                       (SELECT count(*) FROM dead_letters)                                       AS dead_lettered,
                       (SELECT paused FROM queue_control WHERE id = 1)                           AS paused
                FROM jobs
                WHERE state IN ('queued', 'failed', 'running')
                """, (rs, n) -> new QueueDepth(
                rs.getLong("ready"), rs.getLong("delayed"), rs.getLong("retrying"), rs.getLong("running"),
                rs.getLong("dead_lettered"), rs.getDouble("oldest_ready_age"), rs.getBoolean("paused")));
    }

    // ------------------------------------------------------------------ worker path

    /**
     * Atomically claims up to {@code limit} due jobs for {@code workerId}: highest priority first, then oldest.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} means concurrent workers never block on, or receive, the same row.
     * Claiming starts an attempt and takes a lease of {@code timeout + leaseGrace}. Nothing is claimed
     * while the queue is paused.
     */
    public List<Job> claim(String workerId, int limit, long leaseGraceSeconds) {
        if (limit <= 0) {
            return List.of();
        }
        return jdbc.query("""
                WITH picked AS MATERIALIZED (
                    SELECT id
                      FROM jobs
                     WHERE state IN ('queued', 'failed')
                       AND run_at <= now()
                       AND NOT (SELECT paused FROM queue_control WHERE id = 1)
                     ORDER BY priority DESC, run_at, created_at
                     LIMIT ?
                       FOR UPDATE SKIP LOCKED
                )
                UPDATE jobs j
                   SET state = 'running',
                       attempts = j.attempts + 1,
                       locked_by = ?,
                       lease_expires_at = now() + ((j.timeout_seconds + ?) * interval '1 second'),
                       started_at = now(),
                       updated_at = now()
                  FROM picked
                 WHERE j.id = picked.id
                RETURNING j.*
                """, jobMapper, limit, workerId, leaseGraceSeconds)
                .stream()
                // UPDATE ... RETURNING does not preserve the CTE's order.
                .sorted((a, b) -> a.priority() != b.priority()
                        ? Integer.compare(b.priority(), a.priority())
                        : a.runAt().compareTo(b.runAt()))
                .toList();
    }

    /** Records success. Returns false if this worker no longer holds the lease (result is discarded). */
    public boolean succeed(UUID id, String workerId, int attempt, JsonNode result) {
        return jdbc.update("""
                UPDATE jobs
                   SET state = 'succeeded', result = ?::jsonb, last_error = NULL,
                       locked_by = NULL, lease_expires_at = NULL, finished_at = now(), updated_at = now()
                 WHERE id = ? AND state = 'running' AND locked_by = ? AND attempts = ?
                """, json(result), id, workerId, attempt) == 1;
    }

    /**
     * Records a failed attempt. If retries remain (and the failure is retryable) the job goes to
     * {@code failed} and becomes claimable again after {@code retryDelayMillis}; otherwise it moves to
     * {@code dead_lettered} and a row is written to the dead-letter store in the same statement.
     *
     * @return the updated job, or empty if this worker no longer holds the lease
     */
    public Optional<Job> fail(UUID id, String workerId, int attempt, String error, boolean retryable,
                              long retryDelayMillis) {
        return jdbc.query("""
                WITH upd AS (
                    UPDATE jobs
                       SET state = CASE WHEN ? AND attempts <= max_retries THEN 'failed' ELSE 'dead_lettered' END,
                           run_at = CASE WHEN ? AND attempts <= max_retries
                                         THEN now() + (?::bigint * interval '1 millisecond') ELSE run_at END,
                           finished_at = CASE WHEN ? AND attempts <= max_retries THEN NULL ELSE now() END,
                           last_error = ?, locked_by = NULL, lease_expires_at = NULL, updated_at = now()
                     WHERE id = ? AND state = 'running' AND locked_by = ? AND attempts = ?
                    RETURNING *
                ), dl AS (
                    INSERT INTO dead_letters (job_id, type, payload, attempts, last_error, reason)
                    SELECT id, type, payload, attempts, last_error, ? FROM upd WHERE state = 'dead_lettered'
                )
                SELECT * FROM upd
                """, jobMapper,
                retryable, retryable, retryDelayMillis, retryable, error, id, workerId, attempt,
                retryable ? "retries_exhausted" : "non_retryable").stream().findFirst();
    }

    /**
     * Gives a claimed job back without consuming the attempt. Used on graceful shutdown so in-flight work
     * is picked up immediately by another worker instead of waiting for the lease to expire.
     */
    public boolean release(UUID id, String workerId, int attempt) {
        return jdbc.update("""
                UPDATE jobs
                   SET state = CASE WHEN attempts > 1 THEN 'failed' ELSE 'queued' END,
                       attempts = attempts - 1, run_at = now(),
                       locked_by = NULL, lease_expires_at = NULL, updated_at = now()
                 WHERE id = ? AND state = 'running' AND locked_by = ? AND attempts = ?
                """, id, workerId, attempt) == 1;
    }

    /**
     * Reclaims jobs whose lease expired, i.e. whose worker crashed, was partitioned, or stalled. This is
     * the redelivery half of at-least-once. The lost attempt counts against the retry budget, so a job that
     * repeatedly kills its worker (poison pill) ends up dead-lettered rather than looping forever.
     *
     * @return the jobs that were requeued or dead-lettered
     */
    public List<Job> reapExpiredLeases(long backoffBaseMillis, long backoffMaxMillis, int batch) {
        return jdbc.query("""
                WITH expired AS MATERIALIZED (
                    SELECT id FROM jobs
                     WHERE state = 'running' AND lease_expires_at < now()
                     ORDER BY lease_expires_at
                     LIMIT ?
                       FOR UPDATE SKIP LOCKED
                ), upd AS (
                    UPDATE jobs j
                       SET state = CASE WHEN j.attempts <= j.max_retries THEN 'failed' ELSE 'dead_lettered' END,
                           run_at = CASE WHEN j.attempts <= j.max_retries
                                         THEN now() + (LEAST(?::bigint, ?::bigint * power(2, LEAST(j.attempts - 1, 30)))
                                                       * interval '1 millisecond')
                                         ELSE j.run_at END,
                           finished_at = CASE WHEN j.attempts <= j.max_retries THEN NULL ELSE now() END,
                           last_error = 'lease expired on attempt ' || j.attempts || ' (worker ' || j.locked_by
                                        || ' crashed or stalled)',
                           locked_by = NULL, lease_expires_at = NULL, updated_at = now()
                      FROM expired e
                     WHERE j.id = e.id
                    RETURNING j.*
                ), dl AS (
                    INSERT INTO dead_letters (job_id, type, payload, attempts, last_error, reason)
                    SELECT id, type, payload, attempts, last_error, 'lease_expired' FROM upd
                     WHERE state = 'dead_lettered'
                )
                SELECT * FROM upd
                """, jobMapper, batch, backoffMaxMillis, backoffBaseMillis);
    }

    // ------------------------------------------------------------------ operational control

    /** Cancels a job that has not started its next attempt. Running jobs are not interrupted. */
    public CancelResult cancel(UUID id) {
        int updated = jdbc.update("""
                UPDATE jobs SET state = 'cancelled', finished_at = now(), updated_at = now()
                 WHERE id = ? AND\s""" + PENDING, id);
        if (updated == 1) {
            return CancelResult.CANCELLED;
        }
        return find(id).isPresent() ? CancelResult.NOT_CANCELLABLE : CancelResult.NOT_FOUND;
    }

    /** Cancels every pending job (optionally of one type). Returns how many were cancelled. */
    public int drain(String type) {
        String sql = "UPDATE jobs SET state = 'cancelled', finished_at = now(), updated_at = now() WHERE " + PENDING;
        return type == null ? jdbc.update(sql) : jdbc.update(sql + " AND type = ?", type);
    }

    public void setPaused(boolean paused) {
        jdbc.update("UPDATE queue_control SET paused = ?, updated_at = now() WHERE id = 1", paused);
    }

    // ------------------------------------------------------------------ dead-letter store

    public List<DeadLetter> deadLetters(int limit) {
        return jdbc.query("SELECT * FROM dead_letters ORDER BY dead_lettered_at DESC LIMIT ?",
                (rs, n) -> new DeadLetter(
                        rs.getObject("job_id", UUID.class), rs.getString("type"), parse(rs.getString("payload")),
                        rs.getInt("attempts"), rs.getString("last_error"), rs.getString("reason"),
                        instant(rs, "dead_lettered_at")),
                limit);
    }

    /** Puts a dead-lettered job back on the queue with a fresh retry budget. */
    @Transactional
    public Optional<Job> requeueDeadLetter(UUID jobId) {
        List<Job> requeued = jdbc.query("""
                UPDATE jobs
                   SET state = 'queued', attempts = 0, run_at = now(), finished_at = NULL, updated_at = now()
                 WHERE id = ? AND state = 'dead_lettered'
                RETURNING *
                """, jobMapper, jobId);
        if (!requeued.isEmpty()) {
            jdbc.update("DELETE FROM dead_letters WHERE job_id = ?", jobId);
        }
        return requeued.stream().findFirst();
    }

    // ------------------------------------------------------------------ mapping

    private Job mapJob(ResultSet rs, int rowNum) throws SQLException {
        return new Job(
                rs.getObject("id", UUID.class),
                rs.getString("type"),
                parse(rs.getString("payload")),
                rs.getInt("priority"),
                JobState.from(rs.getString("state")),
                rs.getInt("attempts"),
                rs.getInt("max_retries"),
                rs.getInt("timeout_seconds"),
                instant(rs, "run_at"),
                instant(rs, "lease_expires_at"),
                rs.getString("locked_by"),
                rs.getString("last_error"),
                parse(rs.getString("result")),
                rs.getString("idempotency_key"),
                instant(rs, "created_at"),
                instant(rs, "updated_at"),
                instant(rs, "started_at"),
                instant(rs, "finished_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static OffsetDateTime timestamp(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }

    private JsonNode parse(String json) {
        if (json == null) {
            return null;
        }
        try {
            return mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Corrupt JSON in database", e);
        }
    }

    private String json(JsonNode node) {
        return (node == null ? NullNode.getInstance() : node).toString();
    }
}
