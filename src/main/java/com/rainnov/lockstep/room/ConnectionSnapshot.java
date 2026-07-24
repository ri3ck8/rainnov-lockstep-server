package com.rainnov.lockstep.room;

public record ConnectionSnapshot(
    String roomId,
    String matchId,
    String playerId,
    String sessionId,
    boolean takeover,
    boolean reconnected,
    long replayFromFrame,
    long replayToFrame,
    MatchPhase matchPhase
) {
}
