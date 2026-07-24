package com.rainnov.lockstep.transport;

import com.rainnov.lockstep.room.ConnectionSnapshot;
import com.rainnov.lockstep.room.DataPlaneSession;
import com.rainnov.lockstep.room.InputResult;

import java.util.concurrent.CompletionStage;

/**
 * 房间领域面向传输层的边界。
 *
 * <p>所有实现都必须保持房间仅由其事件循环访问；
 * 传输层处理器不得直接修改房间状态。</p>
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
