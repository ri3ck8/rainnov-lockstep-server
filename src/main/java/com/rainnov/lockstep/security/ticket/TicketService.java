package com.rainnov.lockstep.security.ticket;

/**
 * Issues and validates signed player connection tickets.
 */
public interface TicketService {

    /**
     * Issues a deterministic token for the supplied claims.
     *
     * @param claims immutable claims to sign
     * @return a URL-safe token without Base64 padding
     */
    String issue(TicketClaims claims);

    /**
     * Validates a token's structure, signature and temporal claims.
     *
     * @param token token to validate
     * @return the authenticated claims
     * @throws TicketValidationException when validation fails
     */
    TicketClaims validate(String token);
}
