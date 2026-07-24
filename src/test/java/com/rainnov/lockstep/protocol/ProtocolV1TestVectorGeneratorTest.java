package com.rainnov.lockstep.protocol;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Reproducibly generates the cross-engine protocol fixtures.
 *
 * <p>Run with {@code GENERATE_PROTOCOL_VECTORS=true ./gradlew test --tests
 * '*.ProtocolV1TestVectorGeneratorTest'} from the repository root. The regular test suite skips
 * this generator so tests never rewrite tracked fixtures accidentally.
 */
@EnabledIfEnvironmentVariable(named = "GENERATE_PROTOCOL_VECTORS", matches = "true")
class ProtocolV1TestVectorGeneratorTest {

    private static final Path OUTPUT_DIRECTORY =
            Path.of("src", "test", "resources", "protocol-v1");
    private static final HexFormat HEX = HexFormat.of();

    @Test
    void generateCanonicalVectors() throws IOException {
        Files.createDirectories(OUTPUT_DIRECTORY);

        List<Vector> vectors =
                List.of(
                        new Vector(
                                "client-hello.bin",
                                "Envelope.client_hello",
                                """
                                {
                                      "protocol_version": 1,
                                      "request_id": "req-hello-001",
                                      "room_id": "room-01",
                                      "match_id": "match-01",
                                      "player_id": "player-01",
                                      "ticket": "ticket-v1.sample",
                                      "last_applied_frame": 321
                                    }""",
                                clientHello()),
                        new Vector(
                                "client-ping.bin",
                                "Envelope.client_ping",
                                """
                                {
                                      "protocol_version": 1,
                                      "request_id": "req-ping-007",
                                      "sequence": 7
                                    }""",
                                clientPing()),
                        new Vector(
                                "server-pong.bin",
                                "Envelope.server_pong",
                                """
                                {
                                      "protocol_version": 1,
                                      "request_id": "req-ping-007",
                                      "sequence": 7
                                    }""",
                                serverPong()),
                        new Vector(
                                "server-frame.bin",
                                "Envelope.server_frame",
                                """
                                {
                                      "protocol_version": 1,
                                      "request_id": "",
                                      "frame_id": 322,
                                      "inputs": [
                                        {
                                          "player_id": "player-01",
                                          "no_op": false,
                                          "sequence": 42,
                                          "payload_hex": "0102feff"
                                        },
                                        {
                                          "player_id": "player-02",
                                          "no_op": true,
                                          "sequence": 0,
                                          "payload_hex": ""
                                        }
                                      ]
                                    }""",
                                serverFrame()));

        for (Vector vector : vectors) {
            Files.write(OUTPUT_DIRECTORY.resolve(vector.fileName()), vector.envelope().toByteArray());
        }
        Files.writeString(
                OUTPUT_DIRECTORY.resolve("manifest.json"),
                manifest(vectors),
                StandardCharsets.UTF_8);
    }

    private static Envelope clientHello() {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setRequestId("req-hello-001")
                .setClientHello(
                        ClientHello.newBuilder()
                                .setRoomId("room-01")
                                .setMatchId("match-01")
                                .setPlayerId("player-01")
                                .setTicket("ticket-v1.sample")
                                .setLastAppliedFrame(321))
                .build();
    }

    private static Envelope clientPing() {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setRequestId("req-ping-007")
                .setClientPing(ClientPing.newBuilder().setSequence(7))
                .build();
    }

    private static Envelope serverPong() {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setRequestId("req-ping-007")
                .setServerPong(ServerPong.newBuilder().setSequence(7))
                .build();
    }

    private static Envelope serverFrame() {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setServerFrame(
                        ServerFrame.newBuilder()
                                .setFrameId(322)
                                .addInputs(
                                        PlayerFrameInput.newBuilder()
                                                .setPlayerId("player-01")
                                                .setSequence(42)
                                                .setPayload(
                                                        ByteString.copyFrom(
                                                                HEX.parseHex("0102feff"))))
                                .addInputs(
                                        PlayerFrameInput.newBuilder()
                                                .setPlayerId("player-02")
                                                .setNoOp(true)))
                .build();
    }

    private static String manifest(List<Vector> vectors) {
        StringBuilder json =
                new StringBuilder(
                        """
                        {
                          "schema": "lockstep_v1.proto",
                          "protocol_version": 1,
                          "encoding": "protobuf-binary",
                          "vectors": [
                        """);

        for (int index = 0; index < vectors.size(); index++) {
            Vector vector = vectors.get(index);
            byte[] bytes = vector.envelope().toByteArray();
            json.append(
                    """
                                {
                                  "file": "%s",
                                  "message": "%s",
                                  "fields": %s,
                                  "hex": "%s",
                                  "sha256": "%s"
                                }%s
                            """
                            .formatted(
                                    vector.fileName(),
                                    vector.messageName(),
                                    indent(vector.fieldsJson(), 8),
                                    HEX.formatHex(bytes),
                                    sha256(bytes),
                                    index + 1 == vectors.size() ? "" : ","));
        }
        return json.append("  ]\n}\n").toString();
    }

    private static String indent(String value, int spaces) {
        String padding = " ".repeat(spaces);
        return value.replace("\n", "\n" + padding);
    }

    private static String sha256(byte[] value) {
        try {
            return HEX.formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("Every supported JVM must provide SHA-256", exception);
        }
    }

    private record Vector(
            String fileName, String messageName, String fieldsJson, Envelope envelope) {}
}
