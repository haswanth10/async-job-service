package com.asyncjobs.handlers;

import com.asyncjobs.worker.JobContext;
import com.asyncjobs.worker.JobHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.springframework.stereotype.Component;

/** Example handler simulating slow work: sleeps {@code payload.ms} milliseconds. Type {@code sleep}. */
@Component
public class SleepHandler implements JobHandler {

    @Override
    public String type() {
        return "sleep";
    }

    @Override
    public JsonNode handle(JobContext context) throws InterruptedException {
        long millis = context.payload().path("ms").asLong(1000);
        Thread.sleep(millis); // interruptible, so the per-attempt timeout can stop it
        return JsonNodeFactory.instance.objectNode().put("sleptMs", millis);
    }
}
