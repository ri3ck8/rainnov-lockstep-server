package com.rainnov.lockstep.api.dto;

import com.rainnov.lockstep.node.NodeState;

import java.time.Instant;
import java.util.List;

public record NodeCapacityResponse(
    String nodeId,
    NodeState nodeStatus,
    List<DataPlaneEndpointResponse> dataPlaneEndpoints,
    int targetRooms,
    int initializingRooms,
    int readyRooms,
    int activatingRooms,
    int activeRooms,
    int failedRooms,
    int terminatingRooms,
    int healthyRooms,
    int totalLiveRooms,
    Instant sampledAt
) {
    public NodeCapacityResponse {
        dataPlaneEndpoints = List.copyOf(dataPlaneEndpoints);
    }
}
