package com.rainnov.lockstep.api.dto;

import com.rainnov.lockstep.room.MatchPhase;
import com.rainnov.lockstep.room.RoomState;

import java.time.Instant;
import java.util.List;

public record RoomAllocationResponse(
    String allocationId,
    String nodeId,
    String roomId,
    String matchId,
    RoomState roomStatus,
    MatchPhase matchPhase,
    int protocolVersion,
    int tickRate,
    int inputDelayFrames,
    int maxLeadFrames,
    long clientPingIntervalMillis,
    long connectionIdleTimeoutMillis,
    long reconnectGraceMillis,
    Instant joinDeadline,
    List<PlayerTicketResponse> players,
    List<DataPlaneEndpointResponse> dataPlaneEndpoints
) {
    public RoomAllocationResponse {
        players = List.copyOf(players);
        dataPlaneEndpoints = List.copyOf(dataPlaneEndpoints);
    }
}
