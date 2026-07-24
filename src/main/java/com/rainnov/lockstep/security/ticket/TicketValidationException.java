package com.rainnov.lockstep.security.ticket;

import java.util.Objects;

/**
 * Raised when a connection ticket cannot be authenticated or accepted.
 */
public final class TicketValidationException extends RuntimeException {

    public enum Reason {
        MALFORMED_TOKEN,
        INVALID_SIGNATURE,
        INVALID_CLAIMS,
        EXPIRED,
        ISSUED_IN_FUTURE,
        UNSUPPORTED_FORMAT
    }

    private final Reason reason;

    public TicketValidationException(Reason reason, String message) {
        super(message);
        this.reason = Objects.requireNonNull(reason, "reason must not be null");
    }

    public TicketValidationException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = Objects.requireNonNull(reason, "reason must not be null");
    }

    public Reason reason() {
        return reason;
    }

    public Reason getReason() {
        return reason;
    }
}
