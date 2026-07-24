package com.rainnov.lockstep.room;

import java.time.Duration;
import java.util.Objects;

public record RoomSettings(
    int protocolVersion,
    int maxPlayers,
    int tickRate,
    int inputDelayFrames,
    int maxLeadFrames,
    Duration joinTimeout,
    Duration clientPingInterval,
    Duration connectionIdleTimeout,
    Duration reconnectGrace,
    Duration maxDuration,
    int historyFrames,
    int maxPayloadBytes
) {
    public RoomSettings {
        if (protocolVersion <= 0) {
            throw new IllegalArgumentException("protocolVersion must be positive");
        }
        if (maxPlayers <= 0) {
            throw new IllegalArgumentException("maxPlayers must be positive");
        }
        if (tickRate <= 0) {
            throw new IllegalArgumentException("tickRate must be positive");
        }
        if (inputDelayFrames <= 0) {
            throw new IllegalArgumentException("inputDelayFrames must be positive");
        }
        if (maxLeadFrames < inputDelayFrames) {
            throw new IllegalArgumentException("maxLeadFrames must be at least inputDelayFrames");
        }
        joinTimeout = positive(joinTimeout, "joinTimeout");
        clientPingInterval = positive(clientPingInterval, "clientPingInterval");
        connectionIdleTimeout = positive(connectionIdleTimeout, "connectionIdleTimeout");
        reconnectGrace = positive(reconnectGrace, "reconnectGrace");
        maxDuration = positive(maxDuration, "maxDuration");
        if (historyFrames <= 0) {
            throw new IllegalArgumentException("historyFrames must be positive");
        }
        if (maxPayloadBytes <= 0) {
            throw new IllegalArgumentException("maxPayloadBytes must be positive");
        }
        requireUint32Millis(clientPingInterval, "clientPingInterval");
        requireUint32Millis(connectionIdleTimeout, "connectionIdleTimeout");
        requireUint32Millis(reconnectGrace, "reconnectGrace");
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static void requireUint32Millis(Duration value, String name) {
        long millis = value.toMillis();
        if (millis <= 0 || millis > 0xffff_ffffL) {
            throw new IllegalArgumentException(name + " must fit a positive uint32 millisecond value");
        }
    }
}
