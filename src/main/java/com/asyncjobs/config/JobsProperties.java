package com.asyncjobs.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** All tunables, bound from {@code jobs.*}. See application.yml for the matching environment variables. */
@ConfigurationProperties(prefix = "jobs")
public record JobsProperties(
        @DefaultValue Api api,
        @DefaultValue Defaults defaults,
        @DefaultValue Worker worker) {

    public record Api(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("0") long maxQueueDepth,
            @DefaultValue("262144") int maxPayloadBytes) {
    }

    public record Defaults(
            @DefaultValue("0") int priority,
            @DefaultValue("3") int maxRetries,
            @DefaultValue("30") int timeoutSeconds) {
    }

    public record Worker(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("8") int concurrency,
            @DefaultValue("500ms") Duration pollInterval,
            @DefaultValue("15s") Duration leaseGrace,
            @DefaultValue("5s") Duration reapInterval,
            @DefaultValue("25s") Duration shutdownGrace,
            @DefaultValue Backoff backoff) {
    }

    public record Backoff(
            @DefaultValue("1s") Duration base,
            @DefaultValue("5m") Duration max,
            @DefaultValue("true") boolean jitter) {
    }
}
