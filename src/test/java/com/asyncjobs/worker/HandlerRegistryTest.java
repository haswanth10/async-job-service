package com.asyncjobs.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import com.asyncjobs.handlers.EchoHandler;
import com.asyncjobs.handlers.FlakyHandler;

import org.junit.jupiter.api.Test;

class HandlerRegistryTest {

    @Test
    void routesByType() {
        HandlerRegistry registry = new HandlerRegistry(List.of(new EchoHandler(), new FlakyHandler()));

        assertThat(registry.types()).containsExactly("echo", "flaky");
        assertThat(registry.supports("echo")).isTrue();
        assertThat(registry.find("echo")).get().isInstanceOf(EchoHandler.class);
        assertThat(registry.supports("nope")).isFalse();
        assertThat(registry.find("nope")).isEmpty();
    }

    @Test
    void rejectsTwoHandlersForOneType() {
        assertThatThrownBy(() -> new HandlerRegistry(List.of(new EchoHandler(), new EchoHandler())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("echo");
    }
}
