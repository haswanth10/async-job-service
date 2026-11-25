package com.asyncjobs.api;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Body of {@code POST /api/v1/jobs}. Omitted scheduling parameters fall back to {@code jobs.defaults.*}.
 *
 * @param type           which handler runs the job
 * @param payload        arbitrary JSON handed to the handler
 * @param priority       higher runs first (0..100)
 * @param maxRetries     retries after the first attempt (0..25)
 * @param timeoutSeconds time limit for each attempt (1..3600)
 * @param runAt          run no earlier than this instant (ISO-8601)
 * @param delaySeconds   run no earlier than this many seconds from now (added to {@code runAt} if both given)
 */
public record SubmitJobRequest(
        @NotBlank @Size(max = 100) String type,
        JsonNode payload,
        @Min(0) @Max(100) Integer priority,
        @Min(0) @Max(25) Integer maxRetries,
        @Min(1) @Max(3600) Integer timeoutSeconds,
        Instant runAt,
        @Min(0) @Max(31_536_000) Long delaySeconds) {
}
