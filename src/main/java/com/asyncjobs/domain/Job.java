package com.asyncjobs.domain;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

/** A job row. Also the API representation returned by the status endpoint. */
public record Job(
        UUID id,
        String type,
        JsonNode payload,
        int priority,
        JobState state,
        /** Attempts started so far (1 while the first attempt is running). */
        int attempts,
        /** Retries allowed after the first attempt; total attempts = maxRetries + 1. */
        int maxRetries,
        int timeoutSeconds,
        /** Earliest time the next attempt may start. */
        Instant runAt,
        Instant leaseExpiresAt,
        String lockedBy,
        String lastError,
        JsonNode result,
        String idempotencyKey,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant finishedAt) {
}
