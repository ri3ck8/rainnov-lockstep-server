package com.rainnov.lockstep.application;

import java.time.Instant;

public record PlayerTicket(
    String playerId,
    String token,
    Instant expiresAt
) {
}
