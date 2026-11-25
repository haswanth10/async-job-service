package com.asyncjobs.domain;

/**
 * Point-in-time view of the queue.
 *
 * @param ready                 claimable right now (first attempts and due retries)
 * @param delayed               first attempt scheduled in the future
 * @param retrying              failed, waiting out a back-off
 * @param running               attempt in flight
 * @param deadLettered          rows in the dead-letter store
 * @param oldestReadyAgeSeconds how long the oldest claimable job has been waiting; the best saturation signal
 * @param paused                true if workers are currently told not to claim
 */
public record QueueDepth(
        long ready,
        long delayed,
        long retrying,
        long running,
        long deadLettered,
        double oldestReadyAgeSeconds,
        boolean paused) {

    /** Everything that has not started its next attempt yet. */
    @com.fasterxml.jackson.annotation.JsonProperty("pending")
    public long pending() {
        return ready + delayed + retrying;
    }
}
