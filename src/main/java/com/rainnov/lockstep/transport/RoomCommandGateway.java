package com.rainnov.lockstep.transport;

import com.rainnov.lockstep.room.ConnectionSnapshot;
import com.rainnov.lockstep.room.DataPlaneSession;
import com.rainnov.lockstep.room.InputResult;

import java.util.concurrent.CompletionStage;

/**
 * The transport-facing boundary of the room domain.
 *
 * <p>Every implementation must preserve the room's event-loop confinement;
 * transport handlers never mutate room state directly.</p>
 */
interface RoomCommandGateway {

    CompletionStage<ConnectionSnapshot> connect(
        String roomId,
        String matchId,
        String playerId,
        DataPlaneSession session,
        long lastAppliedFrame,
        String requestId
    );

    CompletionStage<InputResult> acceptInput(
        String roomId,
        String playerId,
        String sessionId,
        long targetFrame,
        long sequence,
        byte[] payload,
        String requestId
    );

    CompletionStage<Void> acceptPing(
        String roomId,
        String playerId,
        String sessionId,
        long sequence,
        String requestId
    );

    CompletionStage<Void> disconnect(
        String roomId,
        String playerId,
        String sessionId,
        String reason
    );
}
