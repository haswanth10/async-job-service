package com.asyncjobs.worker;

/** Thrown by a handler when retrying cannot help (bad payload, permanent downstream rejection). */
public class NonRetryableJobException extends RuntimeException {

    public NonRetryableJobException(String message) {
        super(message);
    }

    public NonRetryableJobException(String message, Throwable cause) {
        super(message, cause);
    }
}
