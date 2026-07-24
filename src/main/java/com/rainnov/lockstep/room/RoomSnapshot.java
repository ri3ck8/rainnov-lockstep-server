package com.rainnov.lockstep.room;

import java.time.Instant;
import java.util.List;

public record RoomSnapshot(
    String nodeId,
    String roomId,
    String allocationId,
    String matchId,
    RoomState state,
    MatchPhase matchPhase,
    long currentFrame,
    List<PlayerSnapshot> players,
    Instant createdAt,
    Instant activatedAt,
    Instant startedAt,
    Instant joinDeadline,
    Instant terminatedAt,
    TerminationMode terminationMode,
    TerminationReason terminationReason,
    long lastTickLagNanos
) {
    public RoomSnapshot {
        players = List.copyOf(players);
    }
}
