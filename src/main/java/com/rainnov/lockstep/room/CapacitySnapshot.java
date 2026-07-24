package com.rainnov.lockstep.room;

import java.time.Instant;

public record CapacitySnapshot(
    String nodeId,
    boolean acceptingAllocations,
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
}
