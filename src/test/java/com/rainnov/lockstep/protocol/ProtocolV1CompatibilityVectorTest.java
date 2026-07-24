package com.rainnov.lockstep.protocol;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class ProtocolV1CompatibilityVectorTest {

    private static final HexFormat HEX = HexFormat.of();

    @Test
    void clientHelloVectorIsCanonical() throws IOException {
        byte[] bytes = readVector("client-hello.bin");
        Envelope envelope = Envelope.parseFrom(bytes);

        assertAll(
                () -> assertEquals(1, envelope.getProtocolVersion()),
                () -> assertEquals("req-hello-001", envelope.getRequestId()),
                () -> assertEquals(Envelope.PayloadCase.CLIENT_HELLO, envelope.getPayloadCase()),
                () -> assertEquals("room-01", envelope.getClientHello().getRoomId()),
                () -> assertEquals("match-01", envelope.getClientHello().getMatchId()),
                () -> assertEquals("player-01", envelope.getClientHello().getPlayerId()),
                () -> assertEquals("ticket-v1.sample", envelope.getClientHello().getTicket()),
                () -> assertEquals(321, envelope.getClientHello().getLastAppliedFrame()));

        assertCanonical(
                bytes,
                envelope,
                "0801120d7265712d68656c6c6f2d30303152330a07726f6f6d2d303112086d617463682d30311a09706c617965722d303122107469636b65742d76312e73616d706c6528c102",
                "81220596952f89bda7731f06399691356731e5b65d167509ccec0f7944f1b52a");
    }

    @Test
    void clientPingVectorIsCanonical() throws IOException {
        byte[] bytes = readVector("client-ping.bin");
        Envelope envelope = Envelope.parseFrom(bytes);

        assertAll(
                () -> assertEquals(1, envelope.getProtocolVersion()),
                () -> assertEquals("req-ping-007", envelope.getRequestId()),
                () -> assertEquals(Envelope.PayloadCase.CLIENT_PING, envelope.getPayloadCase()),
                () -> assertEquals(7, envelope.getClientPing().getSequence()));

        assertCanonical(
                bytes,
                envelope,
                "0801120c7265712d70696e672d3030378201020807",
                "0c7c1f6aa62bdab44e319d367bdf18096ff44c445270c934e79164ef603447fe");
    }

    @Test
    void serverPongVectorIsCanonical() throws IOException {
        byte[] bytes = readVector("server-pong.bin");
        Envelope envelope = Envelope.parseFrom(bytes);

        assertAll(
                () -> assertEquals(1, envelope.getProtocolVersion()),
                () -> assertEquals("req-ping-007", envelope.getRequestId()),
                () -> assertEquals(Envelope.PayloadCase.SERVER_PONG, envelope.getPayloadCase()),
                () -> assertEquals(7, envelope.getServerPong().getSequence()));

        assertCanonical(
                bytes,
                envelope,
                "0801120c7265712d70696e672d3030378a01020807",
                "1fc6ded361ec8444e8411097ada3ff18edff9626ef1d4415b76b46f1653de964");
    }

    @Test
    void serverFrameVectorIsCanonical() throws IOException {
        byte[] bytes = readVector("server-frame.bin");
        Envelope envelope = Envelope.parseFrom(bytes);
        ServerFrame frame = envelope.getServerFrame();
        PlayerFrameInput acceptedInput = frame.getInputs(0);
        PlayerFrameInput noOp = frame.getInputs(1);

        assertAll(
                () -> assertEquals(1, envelope.getProtocolVersion()),
                () -> assertTrue(envelope.getRequestId().isEmpty()),
                () -> assertEquals(Envelope.PayloadCase.SERVER_FRAME, envelope.getPayloadCase()),
                () -> assertEquals(322, frame.getFrameId()),
                () -> assertEquals(2, frame.getInputsCount()),
                () -> assertEquals("player-01", acceptedInput.getPlayerId()),
                () -> assertFalse(acceptedInput.getNoOp()),
                () -> assertEquals(42, acceptedInput.getSequence()),
                () ->
                        assertEquals(
                                ByteString.copyFrom(HEX.parseHex("0102feff")),
                                acceptedInput.getPayload()),
                () -> assertEquals("player-02", noOp.getPlayerId()),
                () -> assertTrue(noOp.getNoOp()),
                () -> assertEquals(0, noOp.getSequence()),
                () -> assertTrue(noOp.getPayload().isEmpty()));

        assertCanonical(
                bytes,
                envelope,
                "08016a2708c20212130a09706c617965722d3031182a22040102feff120d0a09706c617965722d30321001",
                "f3af05213e724f25a647bf4714d0908ced8c1f13210395f1cf49ae4674a34054");
    }

    @Test
    void manifestContainsEveryCanonicalVector() throws IOException {
        String manifest = new String(readVector("manifest.json"), StandardCharsets.UTF_8);

        assertAll(
                () -> assertTrue(manifest.contains("\"file\": \"client-hello.bin\"")),
                () ->
                        assertTrue(
                                manifest.contains(
                                        "\"sha256\": \"81220596952f89bda7731f06399691356731e5b65d167509ccec0f7944f1b52a\"")),
                () -> assertTrue(manifest.contains("\"file\": \"client-ping.bin\"")),
                () ->
                        assertTrue(
                                manifest.contains(
                                        "\"sha256\": \"0c7c1f6aa62bdab44e319d367bdf18096ff44c445270c934e79164ef603447fe\"")),
                () -> assertTrue(manifest.contains("\"file\": \"server-pong.bin\"")),
                () ->
                        assertTrue(
                                manifest.contains(
                                        "\"sha256\": \"1fc6ded361ec8444e8411097ada3ff18edff9626ef1d4415b76b46f1653de964\"")),
                () -> assertTrue(manifest.contains("\"file\": \"server-frame.bin\"")),
                () ->
                        assertTrue(
                                manifest.contains(
                                        "\"sha256\": \"f3af05213e724f25a647bf4714d0908ced8c1f13210395f1cf49ae4674a34054\"")));
    }

    private static byte[] readVector(String fileName) throws IOException {
        String resourceName = "/protocol-v1/" + fileName;
        try (InputStream input =
                ProtocolV1CompatibilityVectorTest.class.getResourceAsStream(resourceName)) {
            if (input == null) {
                throw new IOException("Missing test vector resource " + resourceName);
            }
            return input.readAllBytes();
        }
    }

    private static void assertCanonical(
            byte[] original, Envelope parsed, String expectedHex, String expectedSha256) {
        assertAll(
                () -> assertArrayEquals(original, parsed.toByteArray()),
                () -> assertEquals(expectedHex, HEX.formatHex(original)),
                () -> assertEquals(expectedSha256, sha256(original)));
    }

    private static String sha256(byte[] value) {
        try {
            return HEX.formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("Every supported JVM must provide SHA-256", exception);
        }
    }
}
