package com.rainnov.lockstep.room;

import java.time.Instant;
import java.util.List;

public record AllocationSnapshot(
    String allocationId,
    String nodeId,
    String roomId,
    String matchId,
    List<String> playerIds,
    RoomState roomState,
    MatchPhase matchPhase,
    long currentFrame,
    int protocolVersion,
    int tickRate,
    int inputDelayFrames,
    int maxLeadFrames,
    Instant joinDeadline
) {
    public AllocationSnapshot {
        playerIds = List.copyOf(playerIds);
    }
}
