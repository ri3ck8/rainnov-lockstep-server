package com.rainnov.lockstep.api;

import com.rainnov.lockstep.api.dto.DataPlaneEndpointResponse;
import com.rainnov.lockstep.api.dto.NodeCapacityResponse;
import com.rainnov.lockstep.config.LockstepProperties;
import com.rainnov.lockstep.node.NodeLifecycleManager;
import com.rainnov.lockstep.room.RoomPoolManager;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/internal/v1/node")
public final class NodeController {

    private final RoomPoolManager roomPool;
    private final NodeLifecycleManager nodeLifecycle;
    private final LockstepProperties properties;

    public NodeController(
        RoomPoolManager roomPool,
        NodeLifecycleManager nodeLifecycle,
        LockstepProperties properties
    ) {
        this.roomPool = roomPool;
        this.nodeLifecycle = nodeLifecycle;
        this.properties = properties;
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
}
