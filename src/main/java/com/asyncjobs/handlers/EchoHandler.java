package com.asyncjobs.handlers;

import com.asyncjobs.worker.JobContext;
import com.asyncjobs.worker.JobHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.springframework.stereotype.Component;

/** Example handler: returns its payload. Type {@code echo}. */
@Component
public class EchoHandler implements JobHandler {

    @Override
    public String type() {
        return "echo";
    }

    @Override
    public JsonNode handle(JobContext context) {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.set("echo", context.payload());
        result.put("attempt", context.attempt());
        return result;
    }
}
