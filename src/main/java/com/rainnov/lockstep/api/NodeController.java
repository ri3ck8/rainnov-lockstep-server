package com.rainnov.lockstep.api;

import com.rainnov.lockstep.api.dto.DataPlaneEndpointResponse;
import com.rainnov.lockstep.api.dto.NodeCapacityResponse;
import com.rainnov.lockstep.api.dto.NodeRoomsResponse;
import com.rainnov.lockstep.config.LockstepProperties;
import com.rainnov.lockstep.node.NodeLifecycleManager;
import com.rainnov.lockstep.room.RoomPoolManager;
import com.rainnov.lockstep.room.RoomState;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.util.List;

@RestController
@RequestMapping("/internal/v1/node")
public final class NodeController {

    private final RoomPoolManager roomPool;
    private final NodeLifecycleManager nodeLifecycle;
    private final LockstepProperties properties;
    private final Clock clock;

    public NodeController(
        RoomPoolManager roomPool,
        NodeLifecycleManager nodeLifecycle,
        LockstepProperties properties,
        Clock lockstepClock
    ) {
        this.roomPool = roomPool;
        this.nodeLifecycle = nodeLifecycle;
        this.properties = properties;
        this.clock = lockstepClock;
    }

    @GetMapping("/capacity")
    Mono<NodeCapacityResponse> capacity() {
        return Mono.fromCompletionStage(roomPool.capacity()).map(capacity ->
            new NodeCapacityResponse(
                capacity.nodeId(),
                nodeLifecycle.state(),
                List.of(new DataPlaneEndpointResponse(
                    "WEBSOCKET",
                    properties.getDataPlane().getAdvertisedUri().toString(),
                    properties.getDataPlane().getSubprotocol(),
                    "PROTOBUF"
                )),
                capacity.acceptingAllocations(),
                capacity.targetRooms(),
                capacity.initializingRooms(),
                capacity.readyRooms(),
                capacity.activatingRooms(),
                capacity.activeRooms(),
                capacity.failedRooms(),
                capacity.terminatingRooms(),
                capacity.healthyRooms(),
                capacity.totalLiveRooms(),
                capacity.sampledAt()
            )
        );
    }

    @GetMapping("/rooms")
    Mono<NodeRoomsResponse> rooms() {
        return Mono.fromCompletionStage(roomPool.liveSnapshots()).map(items -> {
            var liveItems = items.stream()
                .filter(item -> item.state() != RoomState.TERMINATED)
                .toList();
            return new NodeRoomsResponse(
                properties.getNode().getId(),
                liveItems,
                liveItems.size(),
                clock.instant()
            );
        });
    }
}
