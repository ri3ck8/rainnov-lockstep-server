package com.rainnov.lockstep.security.ticket;

import static com.rainnov.lockstep.security.ticket.TicketValidationException.Reason.EXPIRED;
import static com.rainnov.lockstep.security.ticket.TicketValidationException.Reason.INVALID_SIGNATURE;
import static com.rainnov.lockstep.security.ticket.TicketValidationException.Reason.ISSUED_IN_FUTURE;
import static com.rainnov.lockstep.security.ticket.TicketValidationException.Reason.MALFORMED_TOKEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HmacTicketServiceTest {

    private static final Instant NOW = Instant.parse("2026-07-24T06:00:00.123456789Z");
    private static final byte[] SECRET =
            "a-32-byte-or-longer-test-signing-secret".getBytes(StandardCharsets.UTF_8);

    private HmacTicketService service;
    private TicketClaims claims;

    @BeforeEach
    void setUp() {
        service = new HmacTicketService(SECRET, Clock.fixed(NOW, ZoneOffset.UTC));
        claims = new TicketClaims(
                1,
                "node-ap-southeast-1",
                "room-42",
                "match-9001",
                "player-unity-7",
                NOW.minusSeconds(1),
                NOW.plusSeconds(300)
        );
    }

    @Test
    void issuesAndValidatesTicket() {
        String token = service.issue(claims);

        assertEquals(2, token.split("\\.", -1).length);
        assertEquals(claims, service.validate(token));
    }

    @Test
    void sameClaimsAlwaysProduceSameToken() {
        String first = service.issue(claims);
        String second = service.issue(claims);

        assertEquals(first, second);
    }

    @Test
    void rejectsTamperedToken() {
        String token = service.issue(claims);
        int signatureStart = token.indexOf('.') + 1;
        char replacement = token.charAt(signatureStart) == 'A' ? 'B' : 'A';
        String tampered = token.substring(0, signatureStart)
                + replacement
                + token.substring(signatureStart + 1);

        TicketValidationException exception =
                assertThrows(TicketValidationException.class, () -> service.validate(tampered));

        assertEquals(INVALID_SIGNATURE, exception.reason());
    }

    @Test
    void rejectsTicketSignedWithDifferentKey() {
        HmacTicketService otherService = new HmacTicketService(
                "a-completely-different-signing-secret".getBytes(StandardCharsets.UTF_8),
                Clock.fixed(NOW, ZoneOffset.UTC)
        );

        TicketValidationException exception =
                assertThrows(TicketValidationException.class, () -> otherService.validate(service.issue(claims)));

        assertEquals(INVALID_SIGNATURE, exception.reason());
    }

    @Test
    void rejectsExpiredTicketIncludingAtExactExpiryInstant() {
        TicketClaims expiringClaims = new TicketClaims(
                1,
                "node-1",
                "room-1",
                "match-1",
                "player-1",
                NOW.minusSeconds(60),
                NOW
        );

        TicketValidationException exception =
                assertThrows(TicketValidationException.class, () -> service.validate(service.issue(expiringClaims)));

        assertEquals(EXPIRED, exception.reason());
    }

    @Test
    void rejectsTicketIssuedInFuture() {
        TicketClaims futureClaims = new TicketClaims(
                1,
                "node-1",
                "room-1",
                "match-1",
                "player-1",
                NOW.plusNanos(1),
                NOW.plusSeconds(60)
        );

        TicketValidationException exception =
                assertThrows(TicketValidationException.class, () -> service.validate(service.issue(futureClaims)));

        assertEquals(ISSUED_IN_FUTURE, exception.reason());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "only-one-component",
            ".signature",
            "payload.",
            "payload.signature.extra",
            "payload=.signature",
            "pay*load.signature"
    })
    void rejectsMalformedToken(String token) {
        TicketValidationException exception =
                assertThrows(TicketValidationException.class, () -> service.validate(token));

        assertEquals(MALFORMED_TOKEN, exception.reason());
    }

    @Test
    void rejectsNullToken() {
        TicketValidationException exception =
                assertThrows(TicketValidationException.class, () -> service.validate(null));

        assertEquals(MALFORMED_TOKEN, exception.reason());
    }

    @Test
    void rejectsAuthenticatedPayloadWithTrailingData() throws GeneralSecurityException {
        String token = service.issue(claims);
        int separator = token.indexOf('.');
        byte[] payload = Base64.getUrlDecoder().decode(token.substring(0, separator));
        byte[] payloadWithTrailingData = Arrays.copyOf(payload, payload.length + 1);

        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SECRET, "HmacSHA256"));
        String malformedButAuthenticated = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payloadWithTrailingData)
                + "."
                + Base64.getUrlEncoder().withoutPadding().encodeToString(mac.doFinal(payloadWithTrailingData));

        TicketValidationException exception =
                assertThrows(TicketValidationException.class, () -> service.validate(malformedButAuthenticated));

        assertEquals(MALFORMED_TOKEN, exception.reason());
    }
}
