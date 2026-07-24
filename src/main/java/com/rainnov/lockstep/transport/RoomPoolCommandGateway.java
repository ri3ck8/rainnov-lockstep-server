package com.rainnov.lockstep.transport;

import com.rainnov.lockstep.room.ConnectionSnapshot;
import com.rainnov.lockstep.room.DataPlaneSession;
import com.rainnov.lockstep.room.InputResult;
import com.rainnov.lockstep.room.RoomPoolManager;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

@Component
final class RoomPoolCommandGateway implements RoomCommandGateway {

    private final RoomPoolManager roomPool;

    RoomPoolCommandGateway(RoomPoolManager roomPool) {
        this.roomPool = Objects.requireNonNull(roomPool, "roomPool");
    }

    @Override
    public CompletionStage<ConnectionSnapshot> connect(
        String roomId,
        String matchId,
        String playerId,
        DataPlaneSession session,
        long lastAppliedFrame,
        String requestId
    ) {
        return roomPool.connect(
            roomId,
            matchId,
            playerId,
            session,
            lastAppliedFrame,
            requestId
        );
    }

    @Override
    public CompletionStage<InputResult> acceptInput(
        String roomId,
        String playerId,
        String sessionId,
        long targetFrame,
        long sequence,
        byte[] payload,
        String requestId
    ) {
        return roomPool.acceptInput(
            roomId,
            playerId,
            sessionId,
            targetFrame,
            sequence,
            payload,
            requestId
        );
    }

    @Override
    public CompletionStage<Void> acceptPing(
        String roomId,
        String playerId,
        String sessionId,
        long sequence,
        String requestId
    ) {
        return roomPool.acceptPing(roomId, playerId, sessionId, sequence, requestId);
    }

    @Override
    public CompletionStage<Void> disconnect(
        String roomId,
        String playerId,
        String sessionId,
        String reason
    ) {
        return roomPool.disconnect(roomId, playerId, sessionId, reason);
    }
}
