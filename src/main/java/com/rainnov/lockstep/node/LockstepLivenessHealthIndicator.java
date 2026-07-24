package com.rainnov.lockstep.node;

import com.rainnov.lockstep.transport.NettyDataPlaneServer;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component("lockstepLiveness")
public final class LockstepLivenessHealthIndicator implements ReactiveHealthIndicator {

    private final NodeLifecycleManager nodeLifecycle;
    private final NettyDataPlaneServer dataPlane;

    public LockstepLivenessHealthIndicator(
        NodeLifecycleManager nodeLifecycle,
        NettyDataPlaneServer dataPlane
    ) {
        this.nodeLifecycle = nodeLifecycle;
        this.dataPlane = dataPlane;
    }

    @Override
    public Mono<Health> health() {
        boolean terminalFailure = nodeLifecycle.state() == NodeState.TERMINATED;
        if (!dataPlane.isRunning() || terminalFailure) {
            return Mono.just(Health.down()
                .withDetail("nodeState", nodeLifecycle.state())
                .withDetail("dataPlaneRunning", dataPlane.isRunning())
                .build());
        }
        return Mono.just(Health.up()
            .withDetail("nodeState", nodeLifecycle.state())
            .withDetail("dataPlaneRunning", true)
            .build());
    }
}
