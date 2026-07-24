package com.rainnov.lockstep.security.ticket;

import static com.rainnov.lockstep.security.ticket.TicketValidationException.Reason.EXPIRED;
import static com.rainnov.lockstep.security.ticket.TicketValidationException.Reason.INVALID_CLAIMS;
import static com.rainnov.lockstep.security.ticket.TicketValidationException.Reason.INVALID_SIGNATURE;
import static com.rainnov.lockstep.security.ticket.TicketValidationException.Reason.ISSUED_IN_FUTURE;
import static com.rainnov.lockstep.security.ticket.TicketValidationException.Reason.MALFORMED_TOKEN;
import static com.rainnov.lockstep.security.ticket.TicketValidationException.Reason.UNSUPPORTED_FORMAT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * 确定性且适用于 URL 的 HMAC-SHA256 票据实现。
 *
 * <p>令牌由两个不带填充的 Base64URL 部分组成：
 * {@code payload.signature}。签名用于认证原始二进制载荷。
 * 载荷采用固定且带版本号的线路格式；解析时不接受字段缺失、字段重复或尾随字段。</p>
 */
public final class HmacTicketService implements TicketService {

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final int HMAC_LENGTH = 32;
    private static final int MAGIC = 0x4c544b54; // ASCII 标识：LTKT
    private static final int WIRE_FORMAT_VERSION = 1;
    private static final int MAX_TOKEN_LENGTH = 16_384;
    private static final int MAX_PAYLOAD_LENGTH = 8_192;
    private static final int MAX_FIELD_BYTES = 1_024;

    private static final Base64.Encoder BASE64_URL_ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder BASE64_URL_DECODER = Base64.getUrlDecoder();

    private final SecretKeySpec signingKey;
    private final Clock clock;

    public HmacTicketService(byte[] secret, Clock clock) {
        Objects.requireNonNull(secret, "secret must not be null");
        if (secret.length == 0) {
            throw new IllegalArgumentException("secret must not be empty");
        }
        this.signingKey = new SecretKeySpec(secret.clone(), HMAC_ALGORITHM);
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public String issue(TicketClaims claims) {
        Objects.requireNonNull(claims, "claims must not be null");
        byte[] payload = encodeClaims(claims);
        String encodedPayload = BASE64_URL_ENCODER.encodeToString(payload);
        String encodedSignature = BASE64_URL_ENCODER.encodeToString(sign(payload));
        return encodedPayload + "." + encodedSignature;
    }

    @Override
    public TicketClaims validate(String token) {
        TokenParts tokenParts = splitAndDecode(token);
        byte[] expectedSignature = sign(tokenParts.payload());
        boolean validSignature = MessageDigest.isEqual(expectedSignature, tokenParts.signature());
        if (tokenParts.signature().length != HMAC_LENGTH || !validSignature) {
            throw new TicketValidationException(INVALID_SIGNATURE, "Ticket signature is invalid");
        }

        TicketClaims claims = decodeClaims(tokenParts.payload());
        Instant now = clock.instant();
        if (claims.issuedAt().isAfter(now)) {
            throw new TicketValidationException(ISSUED_IN_FUTURE, "Ticket was issued in the future");
        }
        if (!claims.expiresAt().isAfter(now)) {
            throw new TicketValidationException(EXPIRED, "Ticket has expired");
        }
        return claims;
    }

    private byte[] encodeClaims(TicketClaims claims) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(MAGIC);
                output.writeByte(WIRE_FORMAT_VERSION);
                output.writeInt(claims.version());
                writeString(output, claims.nodeId());
                writeString(output, claims.roomId());
                writeString(output, claims.matchId());
                writeString(output, claims.playerId());
                writeInstant(output, claims.issuedAt());
                writeInstant(output, claims.expiresAt());
            }
            byte[] payload = bytes.toByteArray();
            if (payload.length > MAX_PAYLOAD_LENGTH) {
                throw new IllegalArgumentException("Encoded ticket claims are too large");
            }
            return payload;
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to encode ticket claims", exception);
        }
    }

    private TicketClaims decodeClaims(byte[] payload) {
        if (payload.length == 0 || payload.length > MAX_PAYLOAD_LENGTH) {
            throw malformed("Ticket payload length is invalid", null);
        }

        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            int magic = input.readInt();
            int wireFormat = input.readUnsignedByte();
            if (magic != MAGIC || wireFormat != WIRE_FORMAT_VERSION) {
                throw new TicketValidationException(UNSUPPORTED_FORMAT, "Ticket wire format is not supported");
            }

            int version = input.readInt();
            String nodeId = readString(input, "nodeId");
            String roomId = readString(input, "roomId");
            String matchId = readString(input, "matchId");
            String playerId = readString(input, "playerId");
            Instant issuedAt = readInstant(input, "issuedAt");
            Instant expiresAt = readInstant(input, "expiresAt");
            if (input.read() != -1) {
                throw malformed("Ticket payload contains trailing data", null);
            }

            try {
                return new TicketClaims(version, nodeId, roomId, matchId, playerId, issuedAt, expiresAt);
            } catch (IllegalArgumentException | NullPointerException exception) {
                throw new TicketValidationException(INVALID_CLAIMS, "Ticket claims are invalid", exception);
            }
        } catch (TicketValidationException exception) {
            throw exception;
        } catch (EOFException exception) {
            throw malformed("Ticket payload is truncated", exception);
        } catch (IOException exception) {
            throw malformed("Ticket payload cannot be decoded", exception);
        }
    }

    private TokenParts splitAndDecode(String token) {
        if (token == null || token.isEmpty() || token.length() > MAX_TOKEN_LENGTH) {
            throw malformed("Ticket token is missing or too large", null);
        }
        int separator = token.indexOf('.');
        if (separator <= 0 || separator != token.lastIndexOf('.') || separator == token.length() - 1) {
            throw malformed("Ticket must contain exactly two non-empty components", null);
        }

        String encodedPayload = token.substring(0, separator);
        String encodedSignature = token.substring(separator + 1);
        byte[] payload = decodeCanonicalBase64Url(encodedPayload, "payload");
        byte[] signature = decodeCanonicalBase64Url(encodedSignature, "signature");
        return new TokenParts(payload, signature);
    }

    private byte[] decodeCanonicalBase64Url(String encoded, String componentName) {
        if (encoded.indexOf('=') >= 0) {
            throw malformed("Ticket " + componentName + " must not contain Base64 padding", null);
        }
        try {
            byte[] decoded = BASE64_URL_DECODER.decode(encoded);
            if (!BASE64_URL_ENCODER.encodeToString(decoded).equals(encoded)) {
                throw malformed("Ticket " + componentName + " is not canonical Base64URL", null);
            }
            return decoded;
        } catch (IllegalArgumentException exception) {
            throw malformed("Ticket " + componentName + " is not valid Base64URL", exception);
        }
    }

    private byte[] sign(byte[] payload) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(signingKey);
            return mac.doFinal(payload);
        } catch (NoSuchAlgorithmException | InvalidKeyException exception) {
            throw new IllegalStateException("HMAC-SHA256 is unavailable", exception);
        }
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (encoded.length == 0 || encoded.length > MAX_FIELD_BYTES) {
            throw new IllegalArgumentException("Ticket identifier has an invalid encoded length");
        }
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readString(DataInputStream input, String fieldName) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > MAX_FIELD_BYTES) {
            throw malformed("Ticket " + fieldName + " length is invalid", null);
        }
        byte[] encoded = input.readNBytes(length);
        if (encoded.length != length) {
            throw new EOFException("Truncated " + fieldName);
        }
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded))
                    .toString();
        } catch (CharacterCodingException exception) {
            throw malformed("Ticket " + fieldName + " is not valid UTF-8", exception);
        }
    }

    private static void writeInstant(DataOutputStream output, Instant instant) throws IOException {
        output.writeLong(instant.getEpochSecond());
        output.writeInt(instant.getNano());
    }

    private static Instant readInstant(DataInputStream input, String fieldName) throws IOException {
        long epochSecond = input.readLong();
        int nano = input.readInt();
        if (nano < 0 || nano > 999_999_999) {
            throw malformed("Ticket " + fieldName + " nanoseconds are invalid", null);
        }
        try {
            return Instant.ofEpochSecond(epochSecond, nano);
        } catch (DateTimeException | ArithmeticException exception) {
            throw malformed("Ticket " + fieldName + " is outside the supported range", exception);
        }
    }

    private static TicketValidationException malformed(String message, Throwable cause) {
        return cause == null
                ? new TicketValidationException(MALFORMED_TOKEN, message)
                : new TicketValidationException(MALFORMED_TOKEN, message, cause);
    }

    private record TokenParts(byte[] payload, byte[] signature) {
    }
}
