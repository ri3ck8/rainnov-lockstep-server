package com.rainnov.lockstep.api.dto;

import java.time.Instant;

public record PlayerTicketResponse(
    String playerId,
    String ticket,
    Instant expiresAt
) {
}
