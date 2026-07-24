package com.rainnov.lockstep.room;

public record PlayerSnapshot(
    String playerId,
    PlayerState state,
    String sessionId,
    long lastInputFrame,
    long lastSequence
) {
}
