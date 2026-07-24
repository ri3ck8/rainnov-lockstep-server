package com.rainnov.lockstep.security;

import java.util.Objects;

/**
 * REST、票据和数据面共用的标识符策略。
 * 对不合规值直接拒绝而不做规范化，确保签名和房间索引始终精确对应
 * 可信调用方提供的原始字节。
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
