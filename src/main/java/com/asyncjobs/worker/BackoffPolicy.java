package com.asyncjobs.worker;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Exponential back-off: {@code base * 2^(attempt-1)}, capped at {@code max}.
 *
 * <p>With jitter enabled the delay is drawn uniformly from the upper half of that window
 * ({@code [d/2, d]}, "equal jitter"), which keeps a guaranteed minimum wait while spreading out retries
 * of jobs that failed together so they do not hammer a recovering dependency in lock-step.
 */
public record BackoffPolicy(Duration base, Duration max, boolean jitter) {

    public BackoffPolicy {
        if (base.isNegative() || max.isNegative()) {
            throw new IllegalArgumentException("Back-off durations must not be negative");
        }
        if (max.compareTo(base) < 0) {
            throw new IllegalArgumentException("Back-off max must be >= base");
        }
    }

    /** Delay before the retry that follows failed attempt number {@code attempt} (1-based). */
    public long delayMillis(int attempt) {
        return delayMillis(attempt, () -> ThreadLocalRandom.current().nextDouble());
    }

    long delayMillis(int attempt, DoubleSupplier random) {
        long ceiling = ceilingMillis(attempt);
        if (!jitter) {
            return ceiling;
        }
        long half = ceiling / 2;
        return half + (long) (random.getAsDouble() * (ceiling - half));
    }

    /** The un-jittered delay; also the upper bound of the jittered one. */
    public long ceilingMillis(int attempt) {
        int exponent = Math.min(Math.max(attempt, 1) - 1, 30);
        long baseMillis = base.toMillis();
        long maxMillis = max.toMillis();
        // Overflow-safe: compare before multiplying.
        if (baseMillis > (maxMillis >> exponent)) {
            return maxMillis;
        }
        return Math.min(maxMillis, baseMillis << exponent);
    }
}
