package com.rainnov.lockstep.api.dto;

import com.rainnov.lockstep.room.RoomSnapshot;

import java.time.Instant;
import java.util.List;

public record NodeRoomsResponse(
    String nodeId,
    List<RoomSnapshot> items,
    int total,
    Instant sampledAt
) {
    public NodeRoomsResponse {
        items = List.copyOf(items);
    }
}
