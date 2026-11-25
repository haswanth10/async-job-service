package com.asyncjobs.domain;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Parameters for enqueueing a job.
 *
 * @param runAt       run no earlier than this instant (null = now)
 * @param delayMillis additional delay added to {@code runAt}/now, evaluated on the database clock
 */
public record NewJob(
        String type,
        JsonNode payload,
        int priority,
        int maxRetries,
        int timeoutSeconds,
        Instant runAt,
        long delayMillis,
        String idempotencyKey) {
}
