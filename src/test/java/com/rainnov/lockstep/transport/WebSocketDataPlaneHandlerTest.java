package com.rainnov.lockstep.transport;

import com.rainnov.lockstep.config.LockstepProperties;
import com.rainnov.lockstep.protocol.ClientHello;
import com.rainnov.lockstep.protocol.ClientInput;
import com.rainnov.lockstep.protocol.ClientPing;
import com.rainnov.lockstep.protocol.Envelope;
import com.rainnov.lockstep.protocol.ProtocolErrorCode;
import com.rainnov.lockstep.protocol.ServerPong;
import com.rainnov.lockstep.room.ConnectionSnapshot;
import com.rainnov.lockstep.room.DataPlaneSession;
import com.rainnov.lockstep.room.InputDisposition;
import com.rainnov.lockstep.room.InputResult;
import com.rainnov.lockstep.room.MatchPhase;
import com.rainnov.lockstep.room.SessionCloseCodes;
import com.rainnov.lockstep.security.ticket.HmacTicketService;
import com.rainnov.lockstep.security.ticket.TicketClaims;
import com.rainnov.lockstep.security.ticket.TicketService;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class WebSocketDataPlaneHandlerTest {

    private static final String NODE_ID = "node-1";
    private static final String ROOM_ID = "room-1";
    private static final String MATCH_ID = "match-1";
    private static final String PLAYER_ID = "player-1";
    private static final Instant NOW = Instant.parse("2026-07-24T08:00:00Z");

    private LockstepProperties properties;
    private StubRoomGateway rooms;
    private TicketService tickets;
    private AtomicLong nanoTime;
    private RecordingTelemetry telemetry;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        properties = new LockstepProperties();
        properties.getNode().setId(NODE_ID);
        properties.getDataPlane().setAuthenticationTimeout(Duration.ofSeconds(5));
        properties.getDataPlane().setConnectionIdleTimeout(Duration.ofSeconds(15));
        rooms = new StubRoomGateway();
        tickets = new HmacTicketService(
            "test-ticket-secret".getBytes(java.nio.charset.StandardCharsets.UTF_8),
            Clock.fixed(NOW, ZoneOffset.UTC)
        );
        nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(100));
        telemetry = new RecordingTelemetry();
        channel = new EmbeddedChannel(
            new WebSocketDataPlaneHandler(
                rooms,
                tickets,
                properties,
                telemetry,
                nanoTime::get
            )
        );
        completeHandshake();
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void authenticatesAndRoutesOnePingWhichProducesOnePong() throws Exception {
        authenticate(validTicket(PLAYER_ID));

        write(clientPing(19, "ping-19"));
        channel.runPendingTasks();

        assertThat(rooms.connectCalls).isEqualTo(1);
        assertThat(rooms.pingCalls).isEqualTo(1);
        BinaryWebSocketFrame frame = channel.readOutbound();
        try {
            Envelope response = decode(frame);
            assertThat(response.getRequestId()).isEqualTo("ping-19");
            assertThat(response.getPayloadCase()).isEqualTo(Envelope.PayloadCase.SERVER_PONG);
            assertThat(response.getServerPong().getSequence()).isEqualTo(19);
            assertThat((Object) channel.readOutbound()).isNull();
        } finally {
            frame.release();
        }
    }

    @Test
    void recordsSuccessfulReconnect() {
        rooms.reconnected = true;

        authenticate(validTicket(PLAYER_ID));

        assertThat(telemetry.reconnects).isEqualTo(1);
    }

    @Test
    void routesUnsignedInputValuesAndOpaquePayload() {
        authenticate(validTicket(PLAYER_ID));
        byte[] payload = new byte[] {1, 2, 3, (byte) 0xff};
        Envelope input = Envelope.newBuilder()
            .setProtocolVersion(1)
            .setClientInput(
                ClientInput.newBuilder()
                    .setTargetFrame(-1)
                    .setSequence(-2)
                    .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
            )
            .build();

        write(input);

        assertThat(rooms.inputCalls).isEqualTo(1);
        assertThat(rooms.lastTargetFrame).isEqualTo(0xffff_ffffL);
        assertThat(rooms.lastSequence).isEqualTo(0xffff_fffeL);
        assertThat(rooms.lastPayload).containsExactly(payload);
    }

    @Test
    void acceptsAProtobufBinaryMessageSplitAcrossWebSocketFragments() {
        channel.finishAndReleaseAll();
        channel = new EmbeddedChannel(
            new WebSocketFrameAggregator(properties.getDataPlane().getMaxWebsocketFrameBytes()),
            new WebSocketDataPlaneHandler(rooms, tickets, properties, nanoTime::get)
        );
        completeHandshake();
        Envelope hello = Envelope.newBuilder()
            .setProtocolVersion(1)
            .setClientHello(
                ClientHello.newBuilder()
                    .setRoomId(ROOM_ID)
                    .setMatchId(MATCH_ID)
                    .setPlayerId(PLAYER_ID)
                    .setTicket(validTicket(PLAYER_ID))
            )
            .build();
        byte[] bytes = hello.toByteArray();
        int midpoint = bytes.length / 2;

        channel.writeInbound(
            new BinaryWebSocketFrame(
                false,
                0,
                Unpooled.wrappedBuffer(bytes, 0, midpoint)
            )
        );
        channel.writeInbound(
            new ContinuationWebSocketFrame(
                true,
                0,
                Unpooled.wrappedBuffer(bytes, midpoint, bytes.length - midpoint)
            )
        );
        channel.runPendingTasks();

        assertThat(rooms.connectCalls).isEqualTo(1);
    }

    @Test
    void closesWithMessageTooBigWhenAggregatedFragmentsExceedTheLimit()
        throws Exception {
        channel.finishAndReleaseAll();
        channel = new EmbeddedChannel(
            new WebSocketFrameAggregator(8),
            new WebSocketDataPlaneHandler(rooms, tickets, properties, nanoTime::get)
        );
        completeHandshake();

        channel.writeInbound(
            new BinaryWebSocketFrame(false, 0, Unpooled.wrappedBuffer(new byte[6]))
        );
        channel.writeInbound(
            new ContinuationWebSocketFrame(true, 0, Unpooled.wrappedBuffer(new byte[6]))
        );

        BinaryWebSocketFrame errorFrame = channel.readOutbound();
        CloseWebSocketFrame closeFrame = channel.readOutbound();
        try {
            Envelope error = decode(errorFrame);
            assertThat(error.getProtocolError().getCode())
                .isEqualTo(ProtocolErrorCode.PROTOCOL_ERROR_CODE_MALFORMED_MESSAGE);
            assertThat(closeFrame.statusCode())
                .isEqualTo(WebSocketCloseStatus.MESSAGE_TOO_BIG.code());
        } finally {
            errorFrame.release();
            closeFrame.release();
        }
    }

    @Test
    void rejectsTicketWhoseClaimsDoNotMatchHello() throws Exception {
        authenticate(validTicket("another-player"));

        BinaryWebSocketFrame errorFrame = channel.readOutbound();
        CloseWebSocketFrame closeFrame = channel.readOutbound();
        try {
            Envelope error = decode(errorFrame);
            assertThat(error.getProtocolError().getCode())
                .isEqualTo(ProtocolErrorCode.PROTOCOL_ERROR_CODE_AUTHENTICATION_FAILED);
            assertThat(error.getProtocolError().getFatal()).isTrue();
            assertThat(closeFrame.statusCode())
                .isEqualTo(DataPlaneCloseCodes.AUTHENTICATION_FAILED);
            assertThat(rooms.connectCalls).isZero();
        } finally {
            errorFrame.release();
            closeFrame.release();
        }
    }

    @Test
    void rejectsNonBinaryApplicationMessages() throws Exception {
        authenticate(validTicket(PLAYER_ID));

        channel.writeInbound(new TextWebSocketFrame("not protobuf"));

        BinaryWebSocketFrame errorFrame = channel.readOutbound();
        CloseWebSocketFrame closeFrame = channel.readOutbound();
        try {
            Envelope error = decode(errorFrame);
            assertThat(error.getProtocolError().getCode())
                .isEqualTo(ProtocolErrorCode.PROTOCOL_ERROR_CODE_MALFORMED_MESSAGE);
            assertThat(closeFrame.statusCode())
                .isEqualTo(WebSocketCloseStatus.INVALID_MESSAGE_TYPE.code());
            assertThat(rooms.disconnectCalls).isEqualTo(1);
        } finally {
            errorFrame.release();
            closeFrame.release();
        }
    }

    @Test
    void closesWhenClientHelloIsNotReceivedWithinFiveSeconds() throws Exception {
        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();

        BinaryWebSocketFrame errorFrame = channel.readOutbound();
        CloseWebSocketFrame closeFrame = channel.readOutbound();
        try {
            Envelope error = decode(errorFrame);
            assertThat(error.getProtocolError().getCode())
                .isEqualTo(ProtocolErrorCode.PROTOCOL_ERROR_CODE_HELLO_TIMEOUT);
            assertThat(closeFrame.statusCode()).isEqualTo(DataPlaneCloseCodes.HELLO_TIMEOUT);
        } finally {
            errorFrame.release();
            closeFrame.release();
        }
    }

    @Test
    void closesWhenRoomAuthenticationDoesNotCompleteBeforeDeadline() throws Exception {
        rooms.connectResult = new CompletableFuture<>();
        write(hello(validTicket(PLAYER_ID)));

        assertThat(rooms.connectCalls).isEqualTo(1);
        channel.advanceTimeBy(5, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();

        BinaryWebSocketFrame errorFrame = channel.readOutbound();
        CloseWebSocketFrame closeFrame = channel.readOutbound();
        try {
            Envelope error = decode(errorFrame);
            assertThat(error.getProtocolError().getCode())
                .isEqualTo(ProtocolErrorCode.PROTOCOL_ERROR_CODE_HELLO_TIMEOUT);
            assertThat(closeFrame.statusCode()).isEqualTo(DataPlaneCloseCodes.HELLO_TIMEOUT);
            assertThat(rooms.disconnectCalls).isEqualTo(1);
        } finally {
            errorFrame.release();
            closeFrame.release();
        }
    }

    @Test
    void closesAndNotifiesRoomAfterFifteenSecondsWithoutValidMessages() {
        authenticate(validTicket(PLAYER_ID));
        nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(15));

        channel.advanceTimeBy(15, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();

        CloseWebSocketFrame closeFrame = channel.readOutbound();
        try {
            assertThat(closeFrame.statusCode()).isEqualTo(SessionCloseCodes.HEARTBEAT_TIMEOUT);
            assertThat(closeFrame.reasonText()).isEqualTo("HEARTBEAT_TIMEOUT");
            assertThat(rooms.disconnectCalls).isEqualTo(1);
            assertThat(rooms.lastDisconnectReason).isEqualTo("HEARTBEAT_TIMEOUT");
            assertThat(telemetry.heartbeatTimeouts).isEqualTo(1);
        } finally {
            closeFrame.release();
        }
    }

    @Test
    void validPingRefreshesTheIdleDeadline() {
        authenticate(validTicket(PLAYER_ID));
        nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(10));
        write(clientPing(1, ""));
        releaseOutbound();

        nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(10));
        channel.advanceTimeBy(15, TimeUnit.SECONDS);
        channel.runScheduledPendingTasks();

        assertThat(rooms.disconnectCalls).isZero();
        assertThat(channel.isActive()).isTrue();
    }

    private void completeHandshake() {
        channel.pipeline().fireUserEventTriggered(
            new WebSocketServerProtocolHandler.HandshakeComplete(
                "/game",
                EmptyHttpHeaders.INSTANCE,
                properties.getDataPlane().getSubprotocol()
            )
        );
    }

    private void authenticate(String ticket) {
        write(hello(ticket));
        channel.runPendingTasks();
    }

    private static Envelope hello(String ticket) {
        return Envelope.newBuilder()
            .setProtocolVersion(1)
            .setClientHello(
                ClientHello.newBuilder()
                    .setRoomId(ROOM_ID)
                    .setMatchId(MATCH_ID)
                    .setPlayerId(PLAYER_ID)
                    .setTicket(ticket)
                    .setLastAppliedFrame(0)
            )
            .build();
    }

    private String validTicket(String ticketPlayerId) {
        return tickets.issue(
            new TicketClaims(
                1,
                NODE_ID,
                ROOM_ID,
                MATCH_ID,
                ticketPlayerId,
                NOW.minusSeconds(1),
                NOW.plusSeconds(60)
            )
        );
    }

    private static Envelope clientPing(int sequence, String requestId) {
        return Envelope.newBuilder()
            .setProtocolVersion(1)
            .setRequestId(requestId)
            .setClientPing(ClientPing.newBuilder().setSequence(sequence))
            .build();
    }

    private void write(Envelope envelope) {
        channel.writeInbound(
            new BinaryWebSocketFrame(Unpooled.wrappedBuffer(envelope.toByteArray()))
        );
    }

    private static Envelope decode(BinaryWebSocketFrame frame) throws Exception {
        return Envelope.parseFrom(ByteBufUtil.getBytes(frame.content()));
    }

    private void releaseOutbound() {
        Object message;
        while ((message = channel.readOutbound()) != null) {
            io.netty.util.ReferenceCountUtil.release(message);
        }
    }

    private static final class StubRoomGateway implements RoomCommandGateway {

        private int connectCalls;
        private int inputCalls;
        private int pingCalls;
        private int disconnectCalls;
        private DataPlaneSession session;
        private long lastTargetFrame;
        private long lastSequence;
        private byte[] lastPayload;
        private String lastDisconnectReason;
        private CompletionStage<ConnectionSnapshot> connectResult;
        private boolean reconnected;

        @Override
        public CompletionStage<ConnectionSnapshot> connect(
            String roomId,
            String matchId,
            String playerId,
            DataPlaneSession session,
            long lastAppliedFrame,
            String requestId
        ) {
            connectCalls++;
            this.session = session;
            if (connectResult != null) {
                return connectResult;
            }
            return CompletableFuture.completedFuture(
                new ConnectionSnapshot(
                    roomId,
                    matchId,
                    playerId,
                    session.sessionId(),
                    false,
                    reconnected,
                    0,
                    0,
                    MatchPhase.WAITING_FOR_PLAYERS
                )
            );
        }

        @Override
        public CompletionStage<InputResult> acceptInput(
            String roomId,
            String playerId,
            String sessionId,
            long targetFrame,
            long sequence,
            byte[] payload,
            String requestId
        ) {
            inputCalls++;
            lastTargetFrame = targetFrame;
            lastSequence = sequence;
            lastPayload = payload.clone();
            return CompletableFuture.completedFuture(
                new InputResult(InputDisposition.ACCEPTED, 0, "Accepted")
            );
        }

        @Override
        public CompletionStage<Void> acceptPing(
            String roomId,
            String playerId,
            String sessionId,
            long sequence,
            String requestId
        ) {
            pingCalls++;
            session.send(
                Envelope.newBuilder()
                    .setProtocolVersion(1)
                    .setRequestId(requestId)
                    .setServerPong(ServerPong.newBuilder().setSequence((int) sequence))
                    .build()
            );
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> disconnect(
            String roomId,
            String playerId,
            String sessionId,
            String reason
        ) {
            disconnectCalls++;
            lastDisconnectReason = reason;
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class RecordingTelemetry implements DataPlaneTelemetry {

        private int heartbeatTimeouts;
        private int reconnects;

        @Override
        public void recordHeartbeatTimeout() {
            heartbeatTimeouts++;
        }

        @Override
        public void recordReconnect() {
            reconnects++;
        }
    }
}
