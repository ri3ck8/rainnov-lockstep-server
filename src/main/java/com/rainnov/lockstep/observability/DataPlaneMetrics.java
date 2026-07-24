package com.rainnov.lockstep.observability;

import com.rainnov.lockstep.transport.DataPlaneTelemetry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public final class DataPlaneMetrics implements DataPlaneTelemetry {

    private final Counter heartbeatTimeouts;
    private final Counter reconnects;

    public DataPlaneMetrics(MeterRegistry registry) {
        heartbeatTimeouts = Counter.builder("lockstep.connection.heartbeat.timeouts")
            .description("Authenticated connections closed after inbound idle timeout")
            .register(registry);
        reconnects = Counter.builder("lockstep.connection.reconnects")
            .description("Players that successfully attached a replacement session")
            .register(registry);
    }

    @Override
    public void recordHeartbeatTimeout() {
        heartbeatTimeouts.increment();
    }

    @Override
    public void recordReconnect() {
        reconnects.increment();
    }
}
