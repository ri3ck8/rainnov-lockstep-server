package com.rainnov.lockstep.node;

import com.rainnov.lockstep.room.RoomPoolManager;
import com.rainnov.lockstep.transport.NettyDataPlaneServer;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component("lockstepReadiness")
public final class LockstepReadinessHealthIndicator implements ReactiveHealthIndicator {

    private final NodeLifecycleManager nodeLifecycle;
    private final RoomPoolManager roomPool;
    private final NettyDataPlaneServer dataPlane;

    public LockstepReadinessHealthIndicator(
        NodeLifecycleManager nodeLifecycle,
        RoomPoolManager roomPool,
        NettyDataPlaneServer dataPlane
    ) {
        this.nodeLifecycle = nodeLifecycle;
        this.roomPool = roomPool;
        this.dataPlane = dataPlane;
    }

    @Override
    public Mono<Health> health() {
        if (!nodeLifecycle.isReady() || !dataPlane.isRunning()) {
            return Mono.just(Health.down()
                .withDetail("nodeState", nodeLifecycle.state())
                .withDetail("dataPlaneRunning", dataPlane.isRunning())
                .build());
        }
        return Mono.fromCompletionStage(roomPool.capacity())
            .map(capacity -> {
                Health.Builder health = capacity.acceptingAllocations()
                    ? Health.up()
                    : Health.down();
                return health
                    .withDetail("nodeState", nodeLifecycle.state())
                    .withDetail("dataPlaneRunning", true)
                    .withDetail(
                        "acceptingAllocations",
                        capacity.acceptingAllocations()
                    )
                    .withDetail("readyRooms", capacity.readyRooms())
                    .withDetail("activeRooms", capacity.activeRooms())
                    .withDetail("healthyRooms", capacity.healthyRooms())
                    .build();
            })
            .onErrorResume(error -> Mono.just(Health.down(error).build()));
    }
}
