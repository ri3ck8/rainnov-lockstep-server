package com.rainnov.lockstep.transport;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rainnov.lockstep.config.LockstepProperties;
import com.rainnov.lockstep.protocol.ClientHello;
import com.rainnov.lockstep.protocol.ClientInput;
import com.rainnov.lockstep.protocol.ClientPing;
import com.rainnov.lockstep.protocol.Envelope;
import com.rainnov.lockstep.protocol.ProtocolError;
import com.rainnov.lockstep.protocol.ProtocolErrorCode;
import com.rainnov.lockstep.room.RoomException;
import com.rainnov.lockstep.room.SessionCloseCodes;
import com.rainnov.lockstep.security.ticket.TicketClaims;
import com.rainnov.lockstep.security.ticket.TicketService;
import com.rainnov.lockstep.security.ticket.TicketValidationException;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Decodes the engine-neutral Protobuf protocol and routes authenticated commands
 * to a room. Handler state is confined to the channel event loop.
 */
final class WebSocketDataPlaneHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

    private static final Logger log =
        LoggerFactory.getLogger(WebSocketDataPlaneHandler.class);

    private final RoomCommandGateway rooms;
    private final TicketService tickets;
    private final String nodeId;
    private final String requiredSubprotocol;
    private final int protocolVersion;
    private final long authenticationTimeoutNanos;
    private final long connectionIdleTimeoutNanos;
    private final Duration closeFlushTimeout;
    private final LongSupplier nanoTime;
    private final DataPlaneTelemetry telemetry;

    private SessionState state = SessionState.AWAITING_HANDSHAKE;
    private ScheduledFuture<?> authenticationTimeoutTask;
    private ScheduledFuture<?> idleTimeoutTask;
    private NettyDataPlaneSession session;
    private String roomId;
    private String playerId;
    private long lastInboundNanos;
    private boolean disconnectNotified;

    WebSocketDataPlaneHandler(
        RoomCommandGateway rooms,
        TicketService tickets,
        LockstepProperties properties
    ) {
        this(
            rooms,
            tickets,
            properties,
            DataPlaneTelemetry.NOOP,
            System::nanoTime
        );
    }

    WebSocketDataPlaneHandler(
        RoomCommandGateway rooms,
        TicketService tickets,
        LockstepProperties properties,
        DataPlaneTelemetry telemetry
    ) {
        this(rooms, tickets, properties, telemetry, System::nanoTime);
    }

    WebSocketDataPlaneHandler(
        RoomCommandGateway rooms,
        TicketService tickets,
        LockstepProperties properties,
        LongSupplier nanoTime
    ) {
        this(rooms, tickets, properties, DataPlaneTelemetry.NOOP, nanoTime);
    }

    WebSocketDataPlaneHandler(
        RoomCommandGateway rooms,
        TicketService tickets,
        LockstepProperties properties,
        DataPlaneTelemetry telemetry,
        LongSupplier nanoTime
    ) {
        this.rooms = Objects.requireNonNull(rooms, "rooms");
        this.tickets = Objects.requireNonNull(tickets, "tickets");
        this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        Objects.requireNonNull(properties, "properties");
        this.nodeId = requireText(properties.getNode().getId(), "nodeId");
        this.requiredSubprotocol =
            requireText(properties.getDataPlane().getSubprotocol(), "subprotocol");
        this.protocolVersion = properties.getDataPlane().getProtocolVersion();
        this.authenticationTimeoutNanos = positiveNanos(
            properties.getDataPlane().getAuthenticationTimeout(),
            "authenticationTimeout"
        );
        this.connectionIdleTimeoutNanos = positiveNanos(
            properties.getDataPlane().getConnectionIdleTimeout(),
            "connectionIdleTimeout"
        );
        this.closeFlushTimeout = positive(
            properties.getRoom().getGracefulTerminationTimeout(),
            "gracefulTerminationTimeout"
        );
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext context, Object event)
        throws Exception {
        if (event instanceof WebSocketServerProtocolHandler.HandshakeComplete handshake) {
            onHandshakeComplete(context, handshake);
            return;
        }
        if (event == WebSocketServerProtocolHandler.ServerHandshakeStateEvent.HANDSHAKE_TIMEOUT) {
            state = SessionState.CLOSING;
            context.close();
            return;
        }
        context.fireUserEventTriggered(event);
    }

    private void onHandshakeComplete(
        ChannelHandlerContext context,
        WebSocketServerProtocolHandler.HandshakeComplete handshake
    ) {
        if (state != SessionState.AWAITING_HANDSHAKE) {
            fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_MALFORMED_MESSAGE,
                "Unexpected WebSocket handshake state",
                WebSocketCloseStatus.PROTOCOL_ERROR.code(),
                "PROTOCOL_ERROR"
            );
            return;
        }
        if (!requiredSubprotocol.equals(handshake.selectedSubprotocol())) {
            fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_UNSUPPORTED_PROTOCOL,
                "Required WebSocket subprotocol was not negotiated",
                WebSocketCloseStatus.PROTOCOL_ERROR.code(),
                "UNSUPPORTED_SUBPROTOCOL"
            );
            return;
        }
        state = SessionState.AWAITING_HELLO;
        authenticationTimeoutTask = context.executor().schedule(
            () -> onAuthenticationTimeout(context),
            authenticationTimeoutNanos,
            TimeUnit.NANOSECONDS
        );
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, WebSocketFrame frame) {
        if (!(frame instanceof BinaryWebSocketFrame binary) || !frame.isFinalFragment()) {
            fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_MALFORMED_MESSAGE,
                "Only complete binary WebSocket frames are supported",
                WebSocketCloseStatus.INVALID_MESSAGE_TYPE.code(),
                "BINARY_MESSAGES_REQUIRED"
            );
            return;
        }

        Envelope envelope;
        try {
            envelope = Envelope.parseFrom(ByteBufUtil.getBytes(binary.content()));
        } catch (InvalidProtocolBufferException error) {
            fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_MALFORMED_MESSAGE,
                "Malformed Protobuf envelope",
                WebSocketCloseStatus.INVALID_PAYLOAD_DATA.code(),
                "MALFORMED_MESSAGE"
            );
            return;
        }

        if (envelope.getProtocolVersion() != protocolVersion) {
            fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_UNSUPPORTED_PROTOCOL,
                "Unsupported protocol version",
                WebSocketCloseStatus.PROTOCOL_ERROR.code(),
                "UNSUPPORTED_PROTOCOL",
                envelope.getRequestId()
            );
            return;
        }

        switch (state) {
            case AWAITING_HELLO -> handleHello(context, envelope);
            case AUTHENTICATING -> fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_ALREADY_AUTHENTICATED,
                "Authentication is already in progress",
                WebSocketCloseStatus.POLICY_VIOLATION.code(),
                "ALREADY_AUTHENTICATING",
                envelope.getRequestId()
            );
            case AUTHENTICATED -> handleAuthenticated(context, envelope);
            case AWAITING_HANDSHAKE -> fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_MALFORMED_MESSAGE,
                "WebSocket handshake has not completed",
                WebSocketCloseStatus.PROTOCOL_ERROR.code(),
                "HANDSHAKE_REQUIRED",
                envelope.getRequestId()
            );
            case CLOSING -> {
                // Ignore messages while the close frame is being flushed.
            }
        }
    }

    private void handleHello(ChannelHandlerContext context, Envelope envelope) {
        if (envelope.getPayloadCase() != Envelope.PayloadCase.CLIENT_HELLO) {
            fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_AUTHENTICATION_FAILED,
                "ClientHello must be the first application message",
                DataPlaneCloseCodes.AUTHENTICATION_FAILED,
                "CLIENT_HELLO_REQUIRED",
                envelope.getRequestId()
            );
            return;
        }

        ClientHello hello = envelope.getClientHello();
        state = SessionState.AUTHENTICATING;

        TicketClaims claims;
        try {
            validateHelloFields(hello);
            claims = tickets.validate(hello.getTicket());
            validateClaims(hello, claims);
        } catch (TicketValidationException | IllegalArgumentException error) {
            fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_AUTHENTICATION_FAILED,
                "Connection ticket or ClientHello claims are invalid",
                DataPlaneCloseCodes.AUTHENTICATION_FAILED,
                "AUTHENTICATION_FAILED",
                envelope.getRequestId()
            );
            return;
        }

        NettyDataPlaneSession newSession =
            new NettyDataPlaneSession(
                context.channel(),
                closeFlushTimeout,
                telemetry
            );
        // Publish the connection identity before crossing into the room event loop.
        // If the channel closes while connect is pending, channelInactive can still
        // enqueue a matching disconnect behind the connect command.
        session = newSession;
        roomId = hello.getRoomId();
        playerId = hello.getPlayerId();
        rooms.connect(
            roomId,
            hello.getMatchId(),
            playerId,
            newSession,
            Integer.toUnsignedLong(hello.getLastAppliedFrame()),
            envelope.getRequestId()
        ).whenComplete((connection, error) -> runOnEventLoop(context, () -> {
            if (state != SessionState.AUTHENTICATING || !context.channel().isActive()) {
                return;
            }
            if (error != null) {
                failFromRoom(context, error, envelope.getRequestId());
                return;
            }
            cancel(authenticationTimeoutTask);
            authenticationTimeoutTask = null;
            state = SessionState.AUTHENTICATED;
            if (connection.reconnected()) {
                telemetry.recordReconnect();
            }
            touch();
            scheduleIdleCheck(context, connectionIdleTimeoutNanos);
        }));
    }

    private void handleAuthenticated(ChannelHandlerContext context, Envelope envelope) {
        switch (envelope.getPayloadCase()) {
            case CLIENT_INPUT -> {
                touch();
                routeInput(
                    context,
                    envelope.getClientInput(),
                    envelope.getRequestId()
                );
            }
            case CLIENT_PING -> {
                touch();
                routePing(context, envelope.getClientPing(), envelope.getRequestId());
            }
            case CLIENT_HELLO -> fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_ALREADY_AUTHENTICATED,
                "ClientHello is only valid as the first message",
                WebSocketCloseStatus.POLICY_VIOLATION.code(),
                "ALREADY_AUTHENTICATED",
                envelope.getRequestId()
            );
            default -> fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_MALFORMED_MESSAGE,
                "Message type is not valid in the client-to-server direction",
                WebSocketCloseStatus.POLICY_VIOLATION.code(),
                "INVALID_MESSAGE_DIRECTION",
                envelope.getRequestId()
            );
        }
    }

    private void routeInput(
        ChannelHandlerContext context,
        ClientInput input,
        String requestId
    ) {
        rooms.acceptInput(
            roomId,
            playerId,
            session.sessionId(),
            Integer.toUnsignedLong(input.getTargetFrame()),
            Integer.toUnsignedLong(input.getSequence()),
            input.getPayload().toByteArray(),
            requestId
        ).whenComplete((ignored, error) -> {
            if (error != null) {
                runOnEventLoop(
                    context,
                    () -> failFromRoom(context, error, requestId)
                );
            }
        });
    }

    private void routePing(
        ChannelHandlerContext context,
        ClientPing ping,
        String requestId
    ) {
        rooms.acceptPing(
            roomId,
            playerId,
            session.sessionId(),
            Integer.toUnsignedLong(ping.getSequence()),
            requestId
        ).whenComplete((ignored, error) -> {
            if (error != null) {
                runOnEventLoop(
                    context,
                    () -> failFromRoom(context, error, requestId)
                );
            }
        });
    }

    private void onAuthenticationTimeout(ChannelHandlerContext context) {
        if (state == SessionState.AWAITING_HELLO
            || state == SessionState.AUTHENTICATING) {
            fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_HELLO_TIMEOUT,
                "ClientHello was not received before the authentication deadline",
                DataPlaneCloseCodes.HELLO_TIMEOUT,
                "HELLO_TIMEOUT"
            );
        }
    }

    private void scheduleIdleCheck(ChannelHandlerContext context, long delayNanos) {
        cancel(idleTimeoutTask);
        idleTimeoutTask = context.executor().schedule(
            () -> checkIdle(context),
            Math.max(1, delayNanos),
            TimeUnit.NANOSECONDS
        );
    }

    private void checkIdle(ChannelHandlerContext context) {
        if (state != SessionState.AUTHENTICATED) {
            return;
        }
        long elapsed = nanoTime.getAsLong() - lastInboundNanos;
        if (elapsed < connectionIdleTimeoutNanos) {
            scheduleIdleCheck(context, connectionIdleTimeoutNanos - elapsed);
            return;
        }
        notifyDisconnect("HEARTBEAT_TIMEOUT");
        state = SessionState.CLOSING;
        cancel(idleTimeoutTask);
        idleTimeoutTask = null;
        session.close(SessionCloseCodes.HEARTBEAT_TIMEOUT, "HEARTBEAT_TIMEOUT");
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext context) throws Exception {
        if (state == SessionState.AUTHENTICATED && !context.channel().isWritable()) {
            notifyDisconnect("SLOW_CONSUMER");
            state = SessionState.CLOSING;
            cancel(idleTimeoutTask);
            idleTimeoutTask = null;
            session.close(SessionCloseCodes.SLOW_CONSUMER, "SLOW_CONSUMER");
            return;
        }
        context.fireChannelWritabilityChanged();
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        cancel(authenticationTimeoutTask);
        cancel(idleTimeoutTask);
        authenticationTimeoutTask = null;
        idleTimeoutTask = null;
        notifyDisconnect("CONNECTION_CLOSED");
        state = SessionState.CLOSING;
        context.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        log.debug("Closing data-plane channel after transport failure", cause);
        if (state == SessionState.AWAITING_HANDSHAKE || state == SessionState.CLOSING) {
            state = SessionState.CLOSING;
            context.close();
            return;
        }
        if (cause instanceof TooLongFrameException) {
            fail(
                context,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_MALFORMED_MESSAGE,
                "WebSocket message exceeds the configured size limit",
                WebSocketCloseStatus.MESSAGE_TOO_BIG.code(),
                "MESSAGE_TOO_BIG"
            );
            return;
        }
        fail(
            context,
            ProtocolErrorCode.PROTOCOL_ERROR_CODE_MALFORMED_MESSAGE,
            "Transport failure",
            WebSocketCloseStatus.INTERNAL_SERVER_ERROR.code(),
            "TRANSPORT_FAILURE"
        );
    }

    private void failFromRoom(ChannelHandlerContext context, Throwable error) {
        failFromRoom(context, error, "");
    }

    private void failFromRoom(
        ChannelHandlerContext context,
        Throwable error,
        String requestId
    ) {
        if (state == SessionState.CLOSING) {
            return;
        }
        Throwable cause = unwrap(error);
        ProtocolErrorCode errorCode = ProtocolErrorCode.PROTOCOL_ERROR_CODE_ROOM_NOT_ACTIVE;
        String message = "Room rejected the connection or command";
        int closeCode = DataPlaneCloseCodes.ROOM_REJECTED;
        String closeReason = "ROOM_REJECTED";

        if (cause instanceof RoomException roomError) {
            errorCode = protocolErrorCode(roomError.code());
            message = roomError.getMessage();
            closeReason = roomError.code();
        }
        fail(context, errorCode, message, closeCode, closeReason, requestId);
    }

    private void fail(
        ChannelHandlerContext context,
        ProtocolErrorCode errorCode,
        String message,
        int closeCode,
        String closeReason
    ) {
        fail(context, errorCode, message, closeCode, closeReason, "");
    }

    private void fail(
        ChannelHandlerContext context,
        ProtocolErrorCode errorCode,
        String message,
        int closeCode,
        String closeReason,
        String requestId
    ) {
        if (state == SessionState.CLOSING) {
            return;
        }
        state = SessionState.CLOSING;
        cancel(authenticationTimeoutTask);
        cancel(idleTimeoutTask);
        authenticationTimeoutTask = null;
        idleTimeoutTask = null;
        if (session != null) {
            notifyDisconnect(closeReason);
        }

        Envelope error = Envelope.newBuilder()
            .setProtocolVersion(protocolVersion)
            .setRequestId(requestId == null ? "" : requestId)
            .setProtocolError(
                ProtocolError.newBuilder()
                    .setCode(errorCode)
                    .setMessage(message)
                    .setFatal(true)
            )
            .build();
        NettyDataPlaneSession closingSession = session == null
            ? new NettyDataPlaneSession(context.channel(), closeFlushTimeout)
            : session;
        closingSession.closeAfter(error, closeCode, closeReason);
    }

    private void notifyDisconnect(String reason) {
        if (disconnectNotified || session == null || roomId == null || playerId == null) {
            return;
        }
        disconnectNotified = true;
        rooms.disconnect(roomId, playerId, session.sessionId(), reason)
            .exceptionally(error -> {
                log.debug("Room disconnect notification failed for session {}", session.sessionId());
                return null;
            });
    }

    private void validateClaims(ClientHello hello, TicketClaims claims) {
        if (claims.version() != protocolVersion
            || !nodeId.equals(claims.nodeId())
            || !hello.getRoomId().equals(claims.roomId())
            || !hello.getMatchId().equals(claims.matchId())
            || !hello.getPlayerId().equals(claims.playerId())) {
            throw new IllegalArgumentException("Ticket claims do not match ClientHello");
        }
    }

    private static void validateHelloFields(ClientHello hello) {
        requireText(hello.getRoomId(), "roomId");
        requireText(hello.getMatchId(), "matchId");
        requireText(hello.getPlayerId(), "playerId");
        requireText(hello.getTicket(), "ticket");
    }

    private void touch() {
        lastInboundNanos = nanoTime.getAsLong();
    }

    private static void runOnEventLoop(ChannelHandlerContext context, Runnable command) {
        if (context.executor().inEventLoop()) {
            command.run();
            return;
        }
        try {
            context.executor().execute(command);
        } catch (RuntimeException ignored) {
            context.close();
        }
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException
            || current instanceof ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static ProtocolErrorCode protocolErrorCode(String code) {
        return switch (code) {
            case "ROOM_NOT_ACTIVE", "ROOM_NOT_FOUND", "SESSION_NOT_WRITABLE" ->
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_ROOM_NOT_ACTIVE;
            case "MATCH_MISMATCH" ->
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_MATCH_MISMATCH;
            case "PLAYER_NOT_RESERVED" ->
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_PLAYER_NOT_RESERVED;
            case "REPLAY_HISTORY_EXPIRED" ->
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_REPLAY_HISTORY_EXPIRED;
            default -> ProtocolErrorCode.PROTOCOL_ERROR_CODE_MALFORMED_MESSAGE;
        };
    }

    private static long positiveNanos(Duration value, String name) {
        return positive(value, name).toNanos();
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static void cancel(ScheduledFuture<?> future) {
        if (future != null) {
            future.cancel(false);
        }
    }

    private enum SessionState {
        AWAITING_HANDSHAKE,
        AWAITING_HELLO,
        AUTHENTICATING,
        AUTHENTICATED,
        CLOSING
    }
}
