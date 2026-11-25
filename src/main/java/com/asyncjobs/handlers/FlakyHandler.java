package com.asyncjobs.handlers;

import com.asyncjobs.worker.JobContext;
import com.asyncjobs.worker.JobHandler;
import com.asyncjobs.worker.NonRetryableJobException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.springframework.stereotype.Component;

/**
 * Example handler simulating transient and permanent failures. Type {@code flaky}.
 *
 * <p>Payload: {@code {"failTimes": 2}} fails the first two attempts then succeeds;
 * {@code {"permanent": true}} throws a non-retryable error.
 */
@Component
public class FlakyHandler implements JobHandler {

    @Override
    public String type() {
        return "flaky";
    }

    @Override
    public JsonNode handle(JobContext context) {
        if (context.payload().path("permanent").asBoolean(false)) {
            throw new NonRetryableJobException("permanent failure requested by payload");
        }
        int failTimes = context.payload().path("failTimes").asInt(0);
        if (context.attempt() <= failTimes) {
            throw new IllegalStateException("simulated transient failure on attempt " + context.attempt());
        }
        return JsonNodeFactory.instance.objectNode().put("succeededOnAttempt", context.attempt());
    }
}
