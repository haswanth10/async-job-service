package com.asyncjobs.worker;

import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * What a handler sees for one attempt.
 *
 * @param attempt 1-based attempt number
 */
public record JobContext(UUID jobId, String type, JsonNode payload, int attempt, int maxRetries) {

    public boolean isLastAttempt() {
        return attempt > maxRetries;
    }
}
