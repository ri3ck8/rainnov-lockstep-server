package com.rainnov.lockstep.testclient;

import com.rainnov.lockstep.protocol.PlayerFrameInput;
import com.rainnov.lockstep.protocol.ServerFrame;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final class DeterministicReplica {

    private static final int PAYLOAD_MAGIC = 0x4c535431;
    private static final int PAYLOAD_BYTES = Integer.BYTES * 3;

    private final LinkedHashMap<String, Position> positions = new LinkedHashMap<>();
    private long lastFrame;

    DeterministicReplica(List<String> playerIds) {
        playerIds.forEach(playerId -> positions.put(playerId, new Position()));
    }

    static byte[] command(int playerIndex, long targetFrame) {
        int deltaX = Math.floorMod((int) (targetFrame + playerIndex), 3) - 1;
        int deltaY = Math.floorMod((int) (targetFrame * 2 + playerIndex), 3) - 1;
        return ByteBuffer.allocate(PAYLOAD_BYTES)
            .order(ByteOrder.BIG_ENDIAN)
            .putInt(PAYLOAD_MAGIC)
            .putInt(deltaX)
            .putInt(deltaY)
            .array();
    }

    void apply(ServerFrame frame) {
        long frameId = Integer.toUnsignedLong(frame.getFrameId());
        if (frameId != lastFrame + 1) {
            throw new IllegalStateException(
                "本地模拟期望第 " + (lastFrame + 1) + " 帧，实际为第 " + frameId + " 帧"
            );
        }
        if (frame.getInputsCount() != positions.size()) {
            throw new IllegalStateException(
                "第 " + frameId + " 帧的玩家输入数量不匹配"
            );
        }
        int index = 0;
        for (Map.Entry<String, Position> entry : positions.entrySet()) {
            PlayerFrameInput input = frame.getInputs(index++);
            if (!entry.getKey().equals(input.getPlayerId())) {
                throw new IllegalStateException(
                    "第 " + frameId + " 帧的玩家顺序不稳定"
                );
            }
            if (input.getNoOp()) {
                if (input.getSequence() != 0 || !input.getPayload().isEmpty()) {
                    throw new IllegalStateException(
                        "第 " + frameId + " 帧包含格式错误的空操作"
                    );
                }
                continue;
            }
            ByteBuffer payload = input.getPayload().asReadOnlyByteBuffer()
                .order(ByteOrder.BIG_ENDIAN);
            if (payload.remaining() != PAYLOAD_BYTES
                || payload.getInt() != PAYLOAD_MAGIC) {
                throw new IllegalStateException(
                    "第 " + frameId + " 帧包含无法识别的测试输入"
                );
            }
            int deltaX = payload.getInt();
            int deltaY = payload.getInt();
            if (Math.abs(deltaX) > 1 || Math.abs(deltaY) > 1) {
                throw new IllegalStateException(
                    "第 " + frameId + " 帧包含越界的移动输入"
                );
            }
            entry.getValue().x += deltaX;
            entry.getValue().y += deltaY;
        }
        lastFrame = frameId;
    }

    String stateHash() {
        MessageDigest digest = sha256();
        digest.update(ByteBuffer.allocate(Long.BYTES)
            .order(ByteOrder.BIG_ENDIAN)
            .putLong(lastFrame)
            .array());
        positions.forEach((playerId, position) -> {
            byte[] id = playerId.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(id.length)
                .array());
            digest.update(id);
            digest.update(ByteBuffer.allocate(Long.BYTES * 2)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(position.x)
                .putLong(position.y)
                .array());
        });
        return HexFormat.of().formatHex(digest.digest());
    }

    String positions() {
        return positions.entrySet().stream()
            .map(entry -> entry.getKey() + "=("
                + entry.getValue().x + "," + entry.getValue().y + ")")
            .collect(Collectors.joining(", "));
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("当前 JVM 不支持 SHA-256", error);
        }
    }

    private static final class Position {

        private long x;
        private long y;
    }
}
