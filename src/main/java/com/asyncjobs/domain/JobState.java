package com.asyncjobs.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum JobState {
    /** Waiting for its first attempt (possibly delayed until {@code runAt}). */
    QUEUED("queued"),
    /** Claimed by a worker; an attempt is in flight under a lease. */
    RUNNING("running"),
    /** Terminal: the handler returned and the result was committed. */
    SUCCEEDED("succeeded"),
    /** The last attempt failed; a retry is scheduled at {@code runAt} (back-off). */
    FAILED("failed"),
    /** Terminal: retries exhausted or a non-retryable failure. A copy sits in the dead-letter store. */
    DEAD_LETTERED("dead_lettered"),
    /** Terminal: cancelled or drained by an operator before it ran. */
    CANCELLED("cancelled");

    private final String value;

    JobState(String value) {
        this.value = value;
    }

    @JsonValue
    public String value() {
        return value;
    }

    @JsonCreator
    public static JobState from(String raw) {
        String normalised = raw == null ? "" : raw.trim().toLowerCase().replace('-', '_');
        for (JobState s : values()) {
            if (s.value.equals(normalised)) {
                return s;
            }
        }
        throw new IllegalArgumentException("Unknown job state: " + raw);
    }
}
