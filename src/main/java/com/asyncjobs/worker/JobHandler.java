package com.asyncjobs.worker;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Pluggable unit of work. Register one by declaring a Spring bean; jobs are routed by {@link #type()}.
 *
 * <p>Contract:
 * <ul>
 *   <li><b>Be idempotent.</b> Delivery is at-least-once, so the same job may run more than once.
 *       {@link JobContext#jobId()} is stable across attempts and makes a good de-duplication key for
 *       downstream side effects.</li>
 *   <li><b>Honour interruption.</b> When the per-attempt timeout fires the handler thread is interrupted.
 *       Blocking I/O and {@code Thread.sleep} respond to this; tight CPU loops should check
 *       {@code Thread.currentThread().isInterrupted()}.</li>
 *   <li>Throw any exception to fail the attempt and trigger a retry with back-off. Throw
 *       {@link NonRetryableJobException} to skip the remaining retries and dead-letter immediately.</li>
 * </ul>
 */
public interface JobHandler {

    /** The job type this handler executes, e.g. {@code "send-email"}. Must be unique. */
    String type();

    /** Executes one attempt. The returned value (may be null) is stored as the job's result. */
    JsonNode handle(JobContext context) throws Exception;
}
