package com.rainnov.lockstep.e2e;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rainnov.lockstep.api.dto.PlayerRequest;
import com.rainnov.lockstep.api.dto.PlayerTicketResponse;
import com.rainnov.lockstep.api.dto.RoomAllocationRequest;
import com.rainnov.lockstep.api.dto.RoomAllocationResponse;
import com.rainnov.lockstep.api.dto.RoomTerminationRequest;
import com.rainnov.lockstep.api.dto.RoomTerminationResponse;
import com.rainnov.lockstep.protocol.ClientHello;
import com.rainnov.lockstep.protocol.ClientInput;
import com.rainnov.lockstep.protocol.Envelope;
import com.rainnov.lockstep.protocol.EventType;
import com.rainnov.lockstep.protocol.MatchPhase;
import com.rainnov.lockstep.protocol.PlayerFrameInput;
import com.rainnov.lockstep.room.CapacitySnapshot;
import com.rainnov.lockstep.room.PlayerState;
import com.rainnov.lockstep.room.RoomPoolManager;
import com.rainnov.lockstep.room.RoomSnapshot;
import com.rainnov.lockstep.room.RoomState;
import com.rainnov.lockstep.room.TerminationMode;
import com.rainnov.lockstep.room.TerminationReason;
import com.rainnov.lockstep.security.ApiKeyWebFilter;
import com.rainnov.lockstep.transport.NettyDataPlaneServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "lockstep.node.id=e2e-test-node",
        "lockstep.node.shutdown-grace=0s",
        "lockstep.pool.target-size=1",
        "lockstep.pool.room-executor-threads=1",
        "lockstep.pool.health-check-interval=1h",
        "lockstep.room.join-timeout=10s",
        "lockstep.room.reconnect-grace=10s",
        "lockstep.room.max-duration=5m",
        "lockstep.frame.tick-rate=10",
        "lockstep.frame.history-seconds=10",
        "lockstep.data-plane.port=0",
        "lockstep.data-plane.authentication-timeout=3s",
        "lockstep.data-plane.connection-idle-timeout=30s",
        "lockstep.data-plane.advertised-uri=ws://unused.example.test/game",
        "lockstep.security.api-key=e2e-test-api-key",
        "lockstep.security.ticket-secret=e2e-test-ticket-secret"
    }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class FrameSyncEndToEndTest {

    private static final String API_KEY = "e2e-test-api-key";
    private static final String SUBPROTOCOL = "lockstep.protobuf.v1";
    private static final Duration MESSAGE_TIMEOUT = Duration.ofSeconds(5);

    @LocalServerPort
    private int controlPort;

    @Autowired
    private NettyDataPlaneServer dataPlaneServer;

    @Autowired
    private RoomPoolManager roomPool;

    @Test
    void allocationPlayReconnectTerminateAndReplacementUseRealNetworkPorts()
        throws Exception {
        WebTestClient control = WebTestClient.bindToServer()
            .baseUrl("http://127.0.0.1:" + controlPort)
            .responseTimeout(Duration.ofSeconds(5))
            .build();
        awaitReadyRoom();

        InetSocketAddress dataAddress = dataPlaneServer.localAddress();
        assertThat(dataAddress).isNotNull();
        assertThat(dataAddress.getPort()).isPositive();
        URI gameUri = URI.create("ws://127.0.0.1:" + dataAddress.getPort() + "/game");

        RoomAllocationResponse allocation = allocate(
            control,
            "e2e-allocation-key",
            "e2e-match",
            "player-alpha",
            "player-beta"
        );
        Map<String, PlayerTicketResponse> tickets = allocation.players().stream()
            .collect(Collectors.toMap(PlayerTicketResponse::playerId, ticket -> ticket));

        ProtobufWebSocket alpha = null;
        ProtobufWebSocket beta = null;
        ProtobufWebSocket reconnectedAlpha = null;
        RoomAllocationResponse replacement = null;
        try {
            alpha = ProtobufWebSocket.connect(gameUri, SUBPROTOCOL);
            alpha.send(hello(allocation, tickets.get("player-alpha"), 0));
            Envelope alphaHello = alpha.await(
                envelope -> envelope.hasServerHello(),
                MESSAGE_TIMEOUT
            );
            assertThat(alphaHello.getServerHello().getPlayerId())
                .isEqualTo("player-alpha");
            assertThat(alphaHello.getServerHello().getMatchPhase())
                .isEqualTo(MatchPhase.MATCH_PHASE_WAITING_FOR_PLAYERS);

            beta = ProtobufWebSocket.connect(gameUri, SUBPROTOCOL);
            beta.send(hello(allocation, tickets.get("player-beta"), 0));
            Envelope betaHello = beta.await(
                envelope -> envelope.hasServerHello(),
                MESSAGE_TIMEOUT
            );
            assertThat(betaHello.getServerHello().getPlayerId())
                .isEqualTo("player-beta");

            alpha.await(FrameSyncEndToEndTest::isMatchStarted, MESSAGE_TIMEOUT);
            beta.await(FrameSyncEndToEndTest::isMatchStarted, MESSAGE_TIMEOUT);

            RoomSnapshot running = awaitRoom(
                allocation.roomId(),
                room -> room.matchPhase()
                    == com.rainnov.lockstep.room.MatchPhase.RUNNING
            );
            long targetFrame = running.currentFrame() + allocation.maxLeadFrames();
            byte[] command = "engine-neutral-input".getBytes(StandardCharsets.UTF_8);
            alpha.send(Envelope.newBuilder()
                .setProtocolVersion(allocation.protocolVersion())
                .setRequestId("input-1")
                .setClientInput(ClientInput.newBuilder()
                    .setTargetFrame((int) targetFrame)
                    .setSequence(1)
                    .setPayload(com.google.protobuf.ByteString.copyFrom(command)))
                .build());

            Envelope aggregated = alpha.await(
                envelope -> envelope.hasServerFrame()
                    && Integer.toUnsignedLong(envelope.getServerFrame().getFrameId())
                    == targetFrame,
                MESSAGE_TIMEOUT
            );
            List<PlayerFrameInput> frameInputs = aggregated.getServerFrame().getInputsList();
            assertThat(frameInputs)
                .extracting(PlayerFrameInput::getPlayerId)
                .containsExactly("player-alpha", "player-beta");
            assertThat(frameInputs.get(0).getNoOp()).isFalse();
            assertThat(frameInputs.get(0).getSequence()).isEqualTo(1);
            assertThat(frameInputs.get(0).getPayload().toByteArray()).isEqualTo(command);
            assertThat(frameInputs.get(1).getNoOp()).isTrue();

            alpha.closeNormally();
            awaitRoom(allocation.roomId(), room ->
                playerState(room, "player-alpha") == PlayerState.RECONNECTING
                    && room.currentFrame() >= targetFrame + 2
            );

            reconnectedAlpha = ProtobufWebSocket.connect(gameUri, SUBPROTOCOL);
            reconnectedAlpha.send(hello(
                allocation,
                tickets.get("player-alpha"),
                targetFrame
            ));
            Envelope reconnectHello = reconnectedAlpha.await(
                envelope -> envelope.hasServerHello(),
                MESSAGE_TIMEOUT
            );
            long replayFrom = Integer.toUnsignedLong(
                reconnectHello.getServerHello().getReplayFromFrame()
            );
            long replayTo = Integer.toUnsignedLong(
                reconnectHello.getServerHello().getReplayToFrame()
            );
            assertThat(reconnectHello.getServerHello().getMatchPhase())
                .isEqualTo(MatchPhase.MATCH_PHASE_RUNNING);
            assertThat(replayFrom).isEqualTo(targetFrame + 1);
            assertThat(replayTo).isGreaterThanOrEqualTo(targetFrame + 2);

            List<Long> replayedFrames = new ArrayList<>();
            while (true) {
                Envelope replayMessage = reconnectedAlpha.take(MESSAGE_TIMEOUT);
                if (replayMessage.hasServerFrame()) {
                    replayedFrames.add(Integer.toUnsignedLong(
                        replayMessage.getServerFrame().getFrameId()
                    ));
                }
                if (replayMessage.hasMatchEvent()
                    && replayMessage.getMatchEvent().getType()
                    == EventType.EVENT_TYPE_CATCH_UP_COMPLETED) {
                    break;
                }
            }
            assertThat(replayedFrames).containsExactlyElementsOf(
                inclusiveRange(replayFrom, replayTo)
            );

            RoomTerminationResponse terminated = terminate(
                control,
                allocation.roomId(),
                allocation.matchId()
            );
            assertThat(terminated.roomStatus()).isEqualTo(RoomState.TERMINATED);
            assertThat(terminated.mode()).isEqualTo(TerminationMode.GRACEFUL);
            assertThat(terminated.reason()).isEqualTo(TerminationReason.MATCH_COMPLETED);

            awaitReadyRoom();
            replacement = allocate(
                control,
                "e2e-replacement-key",
                "e2e-replacement-match",
                "replacement-player"
            );
            assertThat(replacement.roomId()).isNotEqualTo(allocation.roomId());

            RoomTerminationResponse replacementTermination = terminate(
                control,
                replacement.roomId(),
                replacement.matchId()
            );
            assertThat(replacementTermination.roomStatus())
                .isEqualTo(RoomState.TERMINATED);
            awaitReadyRoom();
        } finally {
            closeQuietly(reconnectedAlpha);
            closeQuietly(beta);
            closeQuietly(alpha);
            if (replacement != null) {
                terminateQuietly(replacement.roomId(), replacement.matchId());
            }
            terminateQuietly(allocation.roomId(), allocation.matchId());
        }
    }

    private RoomAllocationResponse allocate(
        WebTestClient control,
        String idempotencyKey,
        String matchId,
        String... players
    ) {
        RoomAllocationResponse response = control.post()
            .uri("/api/v1/room-allocations")
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .header("Idempotency-Key", idempotencyKey)
            .bodyValue(new RoomAllocationRequest(
                matchId,
                java.util.Arrays.stream(players).map(PlayerRequest::new).toList()
            ))
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.CREATED)
            .expectBody(RoomAllocationResponse.class)
            .returnResult()
            .getResponseBody();
        assertThat(response).isNotNull();
        return response;
    }

    private RoomTerminationResponse terminate(
        WebTestClient control,
        String roomId,
        String matchId
    ) {
        RoomTerminationResponse response = control.post()
            .uri("/api/v1/rooms/{roomId}/termination", roomId)
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .bodyValue(new RoomTerminationRequest(
                matchId,
                TerminationMode.GRACEFUL,
                TerminationReason.MATCH_COMPLETED
            ))
            .exchange()
            .expectStatus().isAccepted()
            .expectBody(RoomTerminationResponse.class)
            .returnResult()
            .getResponseBody();
        assertThat(response).isNotNull();
        return response;
    }

    private static Envelope hello(
        RoomAllocationResponse allocation,
        PlayerTicketResponse ticket,
        long lastAppliedFrame
    ) {
        Objects.requireNonNull(ticket, "ticket");
        return Envelope.newBuilder()
            .setProtocolVersion(allocation.protocolVersion())
            .setRequestId("hello-" + ticket.playerId())
            .setClientHello(ClientHello.newBuilder()
                .setRoomId(allocation.roomId())
                .setMatchId(allocation.matchId())
                .setPlayerId(ticket.playerId())
                .setTicket(ticket.ticket())
                .setLastAppliedFrame((int) lastAppliedFrame))
            .build();
    }

    private static boolean isMatchStarted(Envelope envelope) {
        return envelope.hasMatchEvent()
            && envelope.getMatchEvent().getType()
            == EventType.EVENT_TYPE_MATCH_STARTED;
    }

    private RoomSnapshot awaitRoom(
        String roomId,
        Predicate<RoomSnapshot> condition
    ) {
        CompletableFuture<RoomSnapshot> matched = new CompletableFuture<>();
        await()
            .atMost(Duration.ofSeconds(5))
            .pollInterval(Duration.ofMillis(20))
            .until(() -> {
                RoomSnapshot snapshot = roomPool.roomSnapshot(roomId)
                    .toCompletableFuture()
                    .get(1, TimeUnit.SECONDS);
                if (condition.test(snapshot)) {
                    matched.complete(snapshot);
                    return true;
                }
                return false;
            });
        return matched.join();
    }

    private void awaitReadyRoom() {
        await()
            .atMost(Duration.ofSeconds(5))
            .pollInterval(Duration.ofMillis(20))
            .untilAsserted(() -> {
                CapacitySnapshot capacity = roomPool.capacity()
                    .toCompletableFuture()
                    .get(1, TimeUnit.SECONDS);
                assertThat(capacity.acceptingAllocations()).isTrue();
                assertThat(capacity.readyRooms()).isEqualTo(1);
                assertThat(capacity.totalLiveRooms()).isEqualTo(1);
            });
    }

    private static PlayerState playerState(RoomSnapshot room, String playerId) {
        return room.players().stream()
            .filter(player -> player.playerId().equals(playerId))
            .findFirst()
            .orElseThrow()
            .state();
    }

    private static List<Long> inclusiveRange(long first, long last) {
        List<Long> values = new ArrayList<>();
        for (long value = first; value <= last; value++) {
            values.add(value);
        }
        return values;
    }

    private void terminateQuietly(String roomId, String matchId) {
        try {
            roomPool.terminate(
                roomId,
                matchId,
                TerminationMode.FORCE,
                TerminationReason.ADMINISTRATIVE
            ).toCompletableFuture().get(2, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // 正常测试流程可能已经终止这个一次性房间。
        }
    }

    private static void closeQuietly(ProtobufWebSocket client) {
        if (client != null) {
            client.close();
        }
    }

    private static final class ProtobufWebSocket
        implements WebSocket.Listener, AutoCloseable {

        private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

        private final BlockingQueue<Object> messages = new LinkedBlockingQueue<>();
        private final CompletableFuture<Integer> closed = new CompletableFuture<>();
        private final ByteArrayOutputStream binaryMessage = new ByteArrayOutputStream();
        private volatile WebSocket socket;

        private static ProtobufWebSocket connect(URI uri, String subprotocol)
            throws Exception {
            ProtobufWebSocket listener = new ProtobufWebSocket();
            WebSocket socket = HTTP_CLIENT.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .subprotocols(subprotocol)
                .buildAsync(uri, listener)
                .get(5, TimeUnit.SECONDS);
            listener.socket = socket;
            assertThat(socket.getSubprotocol()).isEqualTo(subprotocol);
            return listener;
        }

        private void send(Envelope envelope) throws Exception {
            socket.sendBinary(ByteBuffer.wrap(envelope.toByteArray()), true)
                .get(3, TimeUnit.SECONDS);
        }

        private Envelope await(Predicate<Envelope> predicate, Duration timeout)
            throws Exception {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new TimeoutException("Timed out waiting for Protobuf message");
                }
                Envelope message = take(Duration.ofNanos(remaining));
                if (predicate.test(message)) {
                    return message;
                }
            }
        }

        private Envelope take(Duration timeout) throws Exception {
            Object value = messages.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
            if (value == null) {
                throw new TimeoutException("Timed out waiting for WebSocket data");
            }
            if (value instanceof Throwable error) {
                throw new AssertionError("WebSocket listener failed", error);
            }
            return (Envelope) value;
        }

        private void closeNormally() throws Exception {
            WebSocket current = socket;
            if (current == null || closed.isDone()) {
                return;
            }
            current.sendClose(WebSocket.NORMAL_CLOSURE, "reconnect test")
                .get(3, TimeUnit.SECONDS);
            closed.get(3, TimeUnit.SECONDS);
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            socket = webSocket;
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onBinary(
            WebSocket webSocket,
            ByteBuffer data,
            boolean last
        ) {
            byte[] fragment = new byte[data.remaining()];
            data.get(fragment);
            synchronized (binaryMessage) {
                binaryMessage.writeBytes(fragment);
                if (last) {
                    try {
                        messages.offer(Envelope.parseFrom(binaryMessage.toByteArray()));
                    } catch (InvalidProtocolBufferException error) {
                        messages.offer(error);
                    } finally {
                        binaryMessage.reset();
                    }
                }
            }
            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onText(
            WebSocket webSocket,
            CharSequence data,
            boolean last
        ) {
            messages.offer(new AssertionError("Unexpected text WebSocket message: " + data));
            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onClose(
            WebSocket webSocket,
            int statusCode,
            String reason
        ) {
            closed.complete(statusCode);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            messages.offer(error);
            closed.completeExceptionally(error);
        }

        @Override
        public void close() {
            WebSocket current = socket;
            if (current == null || closed.isDone()) {
                return;
            }
            try {
                current.sendClose(WebSocket.NORMAL_CLOSURE, "test cleanup")
                    .get(1, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                current.abort();
            }
        }
    }
}
