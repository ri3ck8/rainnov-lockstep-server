package com.rainnov.lockstep.security;

import java.util.Objects;

/**
 * Shared policy for identifiers that cross REST, tickets and the data plane.
 * Values are rejected rather than normalized so signatures and room indexes
 * always refer to exactly the bytes supplied by the trusted caller.
 */
public final class IdentifierPolicy {

    public static final int MAX_LENGTH = 256;

    private IdentifierPolicy() {
    }

    public static String requireValid(String field, String value) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        if (!value.equals(value.strip())) {
            throw new IllegalArgumentException(
                field + " must not have leading or trailing whitespace"
            );
        }
        if (value.length() > MAX_LENGTH) {
            throw new IllegalArgumentException(
                field + " must not exceed " + MAX_LENGTH + " characters"
            );
        }
        if (value.codePoints().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException(field + " must not contain control characters");
        }
        return value;
    }
}
