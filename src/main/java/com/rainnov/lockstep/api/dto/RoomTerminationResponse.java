package com.rainnov.lockstep.api.dto;

import com.rainnov.lockstep.room.RoomState;
import com.rainnov.lockstep.room.TerminationMode;
import com.rainnov.lockstep.room.TerminationReason;

public record RoomTerminationResponse(
    String roomId,
    String matchId,
    RoomState roomStatus,
    TerminationMode mode,
    TerminationReason reason
) {
}
