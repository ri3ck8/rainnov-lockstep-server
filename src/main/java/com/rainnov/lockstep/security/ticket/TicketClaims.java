package com.rainnov.lockstep.security.ticket;

import com.rainnov.lockstep.security.IdentifierPolicy;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable claims carried by a player connection ticket.
 */
public record TicketClaims(
        int version,
        String nodeId,
        String roomId,
        String matchId,
        String playerId,
        Instant issuedAt,
        Instant expiresAt
) {

    public TicketClaims {
        if (version <= 0) {
            throw new IllegalArgumentException("version must be greater than zero");
        }
        IdentifierPolicy.requireValid("nodeId", nodeId);
        IdentifierPolicy.requireValid("roomId", roomId);
        IdentifierPolicy.requireValid("matchId", matchId);
        IdentifierPolicy.requireValid("playerId", playerId);
        Objects.requireNonNull(issuedAt, "issuedAt must not be null");
        Objects.requireNonNull(expiresAt, "expiresAt must not be null");
        if (!expiresAt.isAfter(issuedAt)) {
            throw new IllegalArgumentException("expiresAt must be after issuedAt");
        }
    }
}
