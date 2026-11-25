package com.asyncjobs.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class BackoffPolicyTest {

    @Test
    void doublesWithEachAttempt() {
        BackoffPolicy policy = new BackoffPolicy(Duration.ofSeconds(1), Duration.ofMinutes(5), false);

        assertThat(policy.delayMillis(1)).isEqualTo(1_000);
        assertThat(policy.delayMillis(2)).isEqualTo(2_000);
        assertThat(policy.delayMillis(3)).isEqualTo(4_000);
        assertThat(policy.delayMillis(4)).isEqualTo(8_000);
    }

    @Test
    void isCappedAtMax() {
        BackoffPolicy policy = new BackoffPolicy(Duration.ofSeconds(1), Duration.ofSeconds(30), false);

        assertThat(policy.delayMillis(6)).isEqualTo(30_000);
        assertThat(policy.delayMillis(20)).isEqualTo(30_000);
    }

    @Test
    void doesNotOverflowForHugeAttemptNumbers() {
        BackoffPolicy policy = new BackoffPolicy(Duration.ofHours(1), Duration.ofDays(365), false);

        assertThat(policy.delayMillis(Integer.MAX_VALUE)).isEqualTo(Duration.ofDays(365).toMillis());
        assertThat(policy.delayMillis(0)).isEqualTo(Duration.ofHours(1).toMillis());
    }

    @Test
    void jitterStaysInUpperHalfOfTheWindow() {
        BackoffPolicy policy = new BackoffPolicy(Duration.ofSeconds(1), Duration.ofMinutes(5), true);

        assertThat(policy.delayMillis(3, () -> 0.0)).isEqualTo(2_000);
        assertThat(policy.delayMillis(3, () -> 0.5)).isEqualTo(3_000);
        assertThat(policy.delayMillis(3, () -> 0.999999)).isBetween(3_999L, 4_000L);
        for (int i = 0; i < 1_000; i++) {
            assertThat(policy.delayMillis(3)).isBetween(2_000L, 4_000L);
        }
    }

    @Test
    void rejectsMaxBelowBase() {
        assertThatThrownBy(() -> new BackoffPolicy(Duration.ofSeconds(10), Duration.ofSeconds(1), false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
