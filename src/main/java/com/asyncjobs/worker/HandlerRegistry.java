package com.asyncjobs.worker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.springframework.stereotype.Component;

/** Maps job types to handlers. Built from every {@link JobHandler} bean in the context. */
@Component
public class HandlerRegistry {

    private final Map<String, JobHandler> handlers = new HashMap<>();

    public HandlerRegistry(List<JobHandler> beans) {
        for (JobHandler handler : beans) {
            JobHandler previous = handlers.put(handler.type(), handler);
            if (previous != null) {
                throw new IllegalStateException("Two handlers registered for job type '" + handler.type() + "': "
                        + previous.getClass().getName() + " and " + handler.getClass().getName());
            }
        }
    }

    public Optional<JobHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public boolean supports(String type) {
        return handlers.containsKey(type);
    }

    public Set<String> types() {
        return new TreeSet<>(handlers.keySet());
    }
}
