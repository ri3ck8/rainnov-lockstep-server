package com.rainnov.lockstep.room;

import com.google.protobuf.ByteString;
import com.rainnov.lockstep.protocol.Envelope;
import com.rainnov.lockstep.protocol.EventType;
import com.rainnov.lockstep.protocol.MatchEvent;
import com.rainnov.lockstep.protocol.PlayerFrameInput;
import com.rainnov.lockstep.protocol.ProtocolError;
import com.rainnov.lockstep.protocol.ProtocolErrorCode;
import com.rainnov.lockstep.protocol.ServerFrame;
import com.rainnov.lockstep.protocol.ServerHello;
import com.rainnov.lockstep.protocol.ServerPong;
import io.netty.util.concurrent.EventExecutor;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * 一次性逻辑游戏房间。所有状态变更均由指定的 {@link EventExecutor} 执行；
 * REST 调用方或 Netty 处理器只负责提交命令。
 */
public final class GameRoom {

    private static final long MAX_UINT32 = 0xffff_ffffL;

    private final String nodeId;
    private final String roomId;
    private final RoomSettings settings;
    private final EventExecutor executor;
    private final RoomTerminalListener terminalListener;
    private final Clock clock;
    private final LongSupplier nanoTime;
    private final Instant createdAt;
    private final long tickPeriodNanos;
    private final LinkedHashMap<String, PlayerSlot> players = new LinkedHashMap<>();
    private final Map<Long, LinkedHashMap<String, PlayerInput>> pendingInputs = new LinkedHashMap<>();
    private final ArrayDeque<Envelope> history = new ArrayDeque<>();

    private volatile RoomSnapshot publishedSnapshot;
    private RoomState state = RoomState.INITIALIZING;
    private MatchPhase matchPhase = MatchPhase.NONE;
    private String allocationId;
    private String matchId;
    private long currentFrame;
    private Instant activatedAt;
    private Instant startedAt;
    private Instant joinDeadline;
    private Instant terminatedAt;
    private TerminationMode terminationMode;
    private TerminationReason terminationReason;
    private ScheduledFuture<?> joinTimeoutTask;
    private ScheduledFuture<?> tickTask;
    private ScheduledFuture<?> maxDurationTask;
    private long nextTickDeadlineNanos;
    private long lastTickNanos;
    private long lastTickLagNanos;
    private boolean terminalCallbackSent;

    public GameRoom(
        String nodeId,
        String roomId,
        RoomSettings settings,
        EventExecutor executor,
        RoomTerminalListener terminalListener
    ) {
        this(nodeId, roomId, settings, executor, terminalListener, Clock.systemUTC(), System::nanoTime);
    }

    GameRoom(
        String nodeId,
        String roomId,
        RoomSettings settings,
        EventExecutor executor,
        RoomTerminalListener terminalListener,
        Clock clock,
        LongSupplier nanoTime
    ) {
        this.nodeId = requireText(nodeId, "nodeId");
        this.roomId = requireText(roomId, "roomId");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.terminalListener = Objects.requireNonNull(terminalListener, "terminalListener");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.createdAt = clock.instant();
        this.tickPeriodNanos = TimeUnit.SECONDS.toNanos(1) / settings.tickRate();
        publishSnapshot();
    }

    public String roomId() {
        return roomId;
    }

    public String matchId() {
        return publishedSnapshot.matchId();
    }

    public RoomState state() {
        return publishedSnapshot.state();
    }

    public RoomSnapshot cachedSnapshot() {
        return publishedSnapshot;
    }

    public EventExecutor eventExecutor() {
        return executor;
    }

    public CompletionStage<RoomSnapshot> initialize() {
        return submit(() -> {
            requireState(RoomState.INITIALIZING);
            state = RoomState.READY;
            publishSnapshot();
            return publishedSnapshot;
        });
    }

    public CompletionStage<AllocationSnapshot> activate(
        String newAllocationId,
        String newMatchId,
        List<String> orderedPlayerIds
    ) {
        Objects.requireNonNull(orderedPlayerIds, "orderedPlayerIds");
        List<String> playerIds = List.copyOf(orderedPlayerIds);
        return submit(() -> activateOnLoop(newAllocationId, newMatchId, playerIds));
    }

    public CompletionStage<ConnectionSnapshot> connect(
        String expectedMatchId,
        String playerId,
        DataPlaneSession session,
        long lastAppliedFrame
    ) {
        return connect(expectedMatchId, playerId, session, lastAppliedFrame, "");
    }

    public CompletionStage<ConnectionSnapshot> connect(
        String expectedMatchId,
        String playerId,
        DataPlaneSession session,
        long lastAppliedFrame,
        String requestId
    ) {
        Objects.requireNonNull(session, "session");
        String responseRequestId = requestId == null ? "" : requestId;
        return submit(() -> connectOnLoop(
            expectedMatchId,
            playerId,
            session,
            lastAppliedFrame,
            responseRequestId
        ));
    }

    public CompletionStage<InputResult> acceptInput(
        String playerId,
        String sessionId,
        long targetFrame,
        long sequence,
        byte[] payload
    ) {
        return acceptInput(playerId, sessionId, targetFrame, sequence, payload, "");
    }

    public CompletionStage<InputResult> acceptInput(
        String playerId,
        String sessionId,
        long targetFrame,
        long sequence,
        byte[] payload,
        String requestId
    ) {
        byte[] copiedPayload = payload == null ? null : payload.clone();
        String responseRequestId = requestId == null ? "" : requestId;
        return submit(() -> acceptInputOnLoop(
            playerId,
            sessionId,
            targetFrame,
            sequence,
            copiedPayload,
            responseRequestId
        ));
    }

    public CompletionStage<Void> acceptPing(
        String playerId,
        String sessionId,
        long sequence,
        String requestId
    ) {
        return submit(() -> {
            PlayerSlot player = requireCurrentSession(playerId, sessionId);
            touch(player);
            Envelope pong = Envelope.newBuilder()
                .setProtocolVersion(settings.protocolVersion())
                .setRequestId(requestId == null ? "" : requestId)
                .setServerPong(ServerPong.newBuilder().setSequence(toUint32(sequence, "sequence")))
                .build();
            sendOrDisconnect(player, pong);
            publishSnapshot();
            return null;
        });
    }

    public CompletionStage<Void> disconnect(String playerId, String sessionId, String reason) {
        return submit(() -> {
            PlayerSlot player = players.get(playerId);
            if (player != null && player.session != null
                && player.session.sessionId().equals(sessionId)
                && state == RoomState.ACTIVE) {
                disconnectOnLoop(player, reason == null ? "CONNECTION_CLOSED" : reason, false);
            }
            return null;
        });
    }

    public CompletionStage<RoomSnapshot> terminate(
        TerminationMode mode,
        TerminationReason reason
    ) {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(reason, "reason");
        return submit(() -> terminateOnLoop(mode, reason, false));
    }

    public CompletionStage<RoomSnapshot> fail(TerminationReason reason) {
        Objects.requireNonNull(reason, "reason");
        return submit(() -> terminateOnLoop(TerminationMode.FORCE, reason, true));
    }

    public CompletionStage<RoomSnapshot> snapshot() {
        return submit(() -> publishedSnapshot);
    }

    public CompletionStage<Boolean> healthCheck() {
        return submit(() -> {
            if (executor.isShuttingDown() || executor.isShutdown() || executor.isTerminated()) {
                return false;
            }
            if (state == RoomState.ACTIVE && matchPhase == MatchPhase.RUNNING) {
                long maximumGap = Math.max(tickPeriodNanos * 4, TimeUnit.SECONDS.toNanos(1));
                return lastTickNanos > 0 && nanoTime.getAsLong() - lastTickNanos <= maximumGap;
            }
            return state != RoomState.FAILED && state != RoomState.TERMINATED;
        });
    }

    private AllocationSnapshot activateOnLoop(
        String newAllocationId,
        String newMatchId,
        List<String> orderedPlayerIds
    ) {
        requireState(RoomState.READY);
        String checkedAllocationId = requireText(newAllocationId, "allocationId");
        String checkedMatchId = requireText(newMatchId, "matchId");
        validatePlayers(orderedPlayerIds);

        state = RoomState.ACTIVATING;
        allocationId = checkedAllocationId;
        matchId = checkedMatchId;
        publishSnapshot();

        try {
            for (String playerId : orderedPlayerIds) {
                players.put(playerId, new PlayerSlot(playerId));
            }
            activatedAt = clock.instant();
            joinDeadline = activatedAt.plus(settings.joinTimeout());
            matchPhase = MatchPhase.WAITING_FOR_PLAYERS;
            state = RoomState.ACTIVE;
            joinTimeoutTask = executor.schedule(
                this::onJoinTimeout,
                settings.joinTimeout().toNanos(),
                TimeUnit.NANOSECONDS
            );
            publishSnapshot();
            return allocationSnapshot();
        } catch (RuntimeException error) {
            terminateOnLoop(TerminationMode.FORCE, TerminationReason.ACTIVATION_FAILED, true);
            throw error;
        }
    }

    private ConnectionSnapshot connectOnLoop(
        String expectedMatchId,
        String playerId,
        DataPlaneSession newSession,
        long lastAppliedFrame,
        String requestId
    ) {
        if (state != RoomState.ACTIVE) {
            throw new RoomException("ROOM_NOT_ACTIVE", "Room is not active");
        }
        if (!Objects.equals(matchId, expectedMatchId)) {
            throw new RoomException("MATCH_MISMATCH", "Match does not belong to this room");
        }
        PlayerSlot player = players.get(playerId);
        if (player == null || player.state == PlayerState.TIMED_OUT
            || player.state == PlayerState.COMPLETED) {
            throw new RoomException("PLAYER_NOT_RESERVED", "Player is not reserved in this room");
        }
        requireUint32(lastAppliedFrame, "lastAppliedFrame");
        if (lastAppliedFrame > currentFrame) {
            throw new RoomException("INVALID_REPLAY_FRAME", "lastAppliedFrame is ahead of the room");
        }

        long replayFrom = 0;
        long replayTo = 0;
        if (lastAppliedFrame < currentFrame) {
            replayFrom = lastAppliedFrame + 1;
            replayTo = currentFrame;
            long earliest = earliestHistoryFrame();
            if (earliest == 0 || replayFrom < earliest) {
                terminateOnLoop(
                    TerminationMode.FORCE,
                    TerminationReason.REPLAY_HISTORY_EXPIRED,
                    true
                );
                throw new RoomException(
                    "REPLAY_HISTORY_EXPIRED",
                    "Required replay history is no longer available"
                );
            }
        }

        boolean takeover = player.session != null
            && !player.session.sessionId().equals(newSession.sessionId());
        DataPlaneSession previous = player.session;
        if (takeover) {
            safeClose(previous, SessionCloseCodes.SESSION_REPLACED, "SESSION_REPLACED");
        }

        boolean reconnect = player.state == PlayerState.RECONNECTING || takeover;
        player.session = newSession;
        player.state = PlayerState.CONNECTED;
        player.connectionGeneration++;
        player.lastInboundNanos = nanoTime.getAsLong();
        cancel(player.reconnectTask);
        player.reconnectTask = null;
        scheduleIdleCheck(player, player.connectionGeneration, settings.connectionIdleTimeout().toNanos());

        Envelope hello = serverHello(playerId, replayFrom, replayTo, requestId);
        sendOrDisconnect(player, hello);
        if (player.state != PlayerState.CONNECTED) {
            throw new RoomException("SESSION_NOT_WRITABLE", "Session is not writable");
        }

        if (replayFrom > 0) {
            for (Envelope historicalFrame : history) {
                long frame = Integer.toUnsignedLong(
                    historicalFrame.getServerFrame().getFrameId()
                );
                if (frame >= replayFrom && frame <= replayTo) {
                    sendOrDisconnect(player, historicalFrame);
                }
            }
            sendOrDisconnect(
                player,
                matchEvent(EventType.EVENT_TYPE_CATCH_UP_COMPLETED, playerId, "", currentFrame)
            );
        }

        if (reconnect) {
            broadcast(matchEvent(
                EventType.EVENT_TYPE_PLAYER_RECONNECTED,
                playerId,
                "",
                currentFrame
            ));
        }
        if (matchPhase == MatchPhase.WAITING_FOR_PLAYERS && allPlayersConnected()) {
            startMatch();
        }
        publishSnapshot();
        return new ConnectionSnapshot(
            roomId,
            matchId,
            playerId,
            newSession.sessionId(),
            takeover,
            reconnect,
            replayFrom,
            replayTo,
            matchPhase
        );
    }

    private InputResult acceptInputOnLoop(
        String playerId,
        String sessionId,
        long targetFrame,
        long sequence,
        byte[] payload,
        String requestId
    ) {
        PlayerSlot player;
        try {
            player = requireCurrentSession(playerId, sessionId);
        } catch (RoomException exception) {
            return new InputResult(
                InputDisposition.REJECTED_SESSION,
                currentFrame,
                exception.getMessage()
            );
        }
        touch(player);
        if (matchPhase != MatchPhase.RUNNING || state != RoomState.ACTIVE) {
            return rejectInput(
                player,
                InputDisposition.REJECTED_NOT_RUNNING,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_ROOM_NOT_ACTIVE,
                "Match is not running",
                requestId
            );
        }
        if (payload == null || payload.length > settings.maxPayloadBytes()) {
            return rejectInput(
                player,
                InputDisposition.REJECTED_TOO_LARGE,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_INPUT_TOO_LARGE,
                "Input payload exceeds the configured limit",
                requestId
            );
        }
        if (!isUint32(targetFrame) || targetFrame < currentFrame + 1
            || targetFrame > currentFrame + settings.maxLeadFrames()) {
            return rejectInput(
                player,
                InputDisposition.REJECTED_TARGET_FRAME,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_INVALID_TARGET_FRAME,
                "targetFrame must be between currentFrame + 1 and currentFrame + maxLeadFrames",
                requestId
            );
        }
        if (!isUint32(sequence) || sequence == 0) {
            return rejectInput(
                player,
                InputDisposition.REJECTED_SEQUENCE,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_DUPLICATE_INPUT,
                "sequence must be a non-zero uint32",
                requestId
            );
        }

        LinkedHashMap<String, PlayerInput> frameInputs =
            pendingInputs.computeIfAbsent(targetFrame, ignored -> new LinkedHashMap<>());
        PlayerInput existing = frameInputs.get(playerId);
        if (existing != null) {
            if (existing.sequence == sequence && Arrays.equals(existing.payload, payload)) {
                return new InputResult(
                    InputDisposition.DUPLICATE_IGNORED,
                    currentFrame,
                    "Identical retry ignored"
                );
            }
            return rejectInput(
                player,
                InputDisposition.REJECTED_CONFLICT,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_DUPLICATE_INPUT,
                "A different input is already accepted for this player and frame",
                requestId
            );
        }
        if (sequence <= player.lastSequence) {
            return rejectInput(
                player,
                InputDisposition.REJECTED_SEQUENCE,
                ProtocolErrorCode.PROTOCOL_ERROR_CODE_DUPLICATE_INPUT,
                "sequence must increase monotonically",
                requestId
            );
        }

        frameInputs.put(playerId, new PlayerInput(sequence, payload.clone()));
        player.lastSequence = sequence;
        player.lastInputFrame = targetFrame;
        publishSnapshot();
        return new InputResult(InputDisposition.ACCEPTED, currentFrame, "Accepted");
    }

    private InputResult rejectInput(
        PlayerSlot player,
        InputDisposition disposition,
        ProtocolErrorCode code,
        String message,
        String requestId
    ) {
        Envelope error = Envelope.newBuilder()
            .setProtocolVersion(settings.protocolVersion())
            .setRequestId(requestId)
            .setProtocolError(
                ProtocolError.newBuilder()
                    .setCode(code)
                    .setMessage(message)
                    .setFatal(false)
            )
            .build();
        sendOrDisconnect(player, error);
        return new InputResult(disposition, currentFrame, message);
    }

    private void startMatch() {
        cancel(joinTimeoutTask);
        joinTimeoutTask = null;
        matchPhase = MatchPhase.RUNNING;
        startedAt = clock.instant();
        lastTickNanos = nanoTime.getAsLong();
        broadcast(matchEvent(EventType.EVENT_TYPE_MATCH_STARTED, "", "", 0));
        nextTickDeadlineNanos = nanoTime.getAsLong() + tickPeriodNanos;
        tickTask = executor.schedule(this::runTick, tickPeriodNanos, TimeUnit.NANOSECONDS);
        maxDurationTask = executor.schedule(
            () -> terminateOnLoop(
                TerminationMode.GRACEFUL,
                TerminationReason.MAX_DURATION,
                false
            ),
            settings.maxDuration().toNanos(),
            TimeUnit.NANOSECONDS
        );
    }

    private void runTick() {
        if (state != RoomState.ACTIVE || matchPhase != MatchPhase.RUNNING) {
            return;
        }
        long now = nanoTime.getAsLong();
        lastTickLagNanos = Math.max(0, now - nextTickDeadlineNanos);
        lastTickNanos = now;
        if (currentFrame == MAX_UINT32) {
            terminateOnLoop(TerminationMode.FORCE, TerminationReason.INTERNAL_ERROR, true);
            return;
        }

        currentFrame++;
        LinkedHashMap<String, PlayerInput> inputs = pendingInputs.remove(currentFrame);
        ServerFrame.Builder frame = ServerFrame.newBuilder().setFrameId((int) currentFrame);
        for (PlayerSlot player : players.values()) {
            PlayerInput input = inputs == null ? null : inputs.get(player.playerId);
            PlayerFrameInput.Builder playerInput = PlayerFrameInput.newBuilder()
                .setPlayerId(player.playerId);
            if (input == null) {
                playerInput.setNoOp(true).setSequence(0).setPayload(ByteString.EMPTY);
            } else {
                playerInput
                    .setNoOp(false)
                    .setSequence((int) input.sequence)
                    .setPayload(ByteString.copyFrom(input.payload));
            }
            frame.addInputs(playerInput);
        }

        Envelope envelope = Envelope.newBuilder()
            .setProtocolVersion(settings.protocolVersion())
            .setServerFrame(frame)
            .build();
        history.addLast(envelope);
        while (history.size() > settings.historyFrames()) {
            history.removeFirst();
        }
        broadcast(envelope);
        publishSnapshot();

        nextTickDeadlineNanos = nanoTime.getAsLong() + tickPeriodNanos;
        tickTask = executor.schedule(this::runTick, tickPeriodNanos, TimeUnit.NANOSECONDS);
    }

    private void onJoinTimeout() {
        if (state == RoomState.ACTIVE && matchPhase == MatchPhase.WAITING_FOR_PLAYERS) {
            for (PlayerSlot player : players.values()) {
                if (player.state != PlayerState.CONNECTED) {
                    player.state = PlayerState.TIMED_OUT;
                }
            }
            terminateOnLoop(TerminationMode.GRACEFUL, TerminationReason.JOIN_TIMEOUT, false);
        }
    }

    private void scheduleIdleCheck(PlayerSlot player, long generation, long delayNanos) {
        cancel(player.idleTask);
        player.idleTask = executor.schedule(
            () -> checkIdle(player, generation),
            Math.max(1, delayNanos),
            TimeUnit.NANOSECONDS
        );
    }

    private void checkIdle(PlayerSlot player, long generation) {
        if (state != RoomState.ACTIVE || player.connectionGeneration != generation
            || player.state != PlayerState.CONNECTED || player.session == null) {
            return;
        }
        long elapsed = nanoTime.getAsLong() - player.lastInboundNanos;
        long idleNanos = settings.connectionIdleTimeout().toNanos();
        if (elapsed < idleNanos) {
            scheduleIdleCheck(player, generation, idleNanos - elapsed);
            return;
        }
        disconnectOnLoop(player, "HEARTBEAT_TIMEOUT", true);
    }

    private void disconnectOnLoop(PlayerSlot player, String reason, boolean closeSession) {
        if (state != RoomState.ACTIVE || player.state != PlayerState.CONNECTED) {
            return;
        }
        DataPlaneSession oldSession = player.session;
        player.session = null;
        player.state = PlayerState.RECONNECTING;
        player.connectionGeneration++;
        cancel(player.idleTask);
        player.idleTask = null;
        if (closeSession) {
            safeClose(oldSession, SessionCloseCodes.HEARTBEAT_TIMEOUT, reason);
        }
        broadcast(matchEvent(
            EventType.EVENT_TYPE_PLAYER_DISCONNECTED,
            player.playerId,
            reason,
            currentFrame
        ));
        long generation = player.connectionGeneration;
        player.reconnectTask = executor.schedule(
            () -> onReconnectTimeout(player, generation),
            settings.reconnectGrace().toNanos(),
            TimeUnit.NANOSECONDS
        );
        publishSnapshot();
    }

    private void onReconnectTimeout(PlayerSlot player, long generation) {
        if (state == RoomState.ACTIVE
            && player.connectionGeneration == generation
            && player.state == PlayerState.RECONNECTING) {
            player.state = PlayerState.TIMED_OUT;
            terminateOnLoop(
                TerminationMode.GRACEFUL,
                TerminationReason.RECONNECT_TIMEOUT,
                false
            );
        }
    }

    private RoomSnapshot terminateOnLoop(
        TerminationMode mode,
        TerminationReason reason,
        boolean failure
    ) {
        if (state == RoomState.TERMINATED) {
            return publishedSnapshot;
        }
        if (state == RoomState.TERMINATING) {
            return publishedSnapshot;
        }

        if (failure) {
            state = RoomState.FAILED;
            publishSnapshot();
        }
        state = RoomState.TERMINATING;
        matchPhase = MatchPhase.FINISHED;
        terminationMode = mode;
        terminationReason = reason;
        cancel(joinTimeoutTask);
        cancel(tickTask);
        cancel(maxDurationTask);
        joinTimeoutTask = null;
        tickTask = null;
        maxDurationTask = null;

        if (mode == TerminationMode.GRACEFUL) {
            broadcast(matchEvent(
                EventType.EVENT_TYPE_MATCH_TERMINATING,
                "",
                reason.name(),
                currentFrame
            ));
            broadcast(matchEvent(
                EventType.EVENT_TYPE_MATCH_ENDED,
                "",
                reason.name(),
                currentFrame
            ));
        }
        for (PlayerSlot player : players.values()) {
            cancel(player.idleTask);
            cancel(player.reconnectTask);
            if (player.session != null) {
                safeClose(
                    player.session,
                    SessionCloseCodes.ROOM_TERMINATED,
                    reason.name()
                );
                player.session = null;
            }
            if (player.state != PlayerState.TIMED_OUT) {
                player.state = PlayerState.COMPLETED;
            }
        }
        pendingInputs.clear();
        history.clear();
        terminatedAt = clock.instant();
        state = RoomState.TERMINATED;
        publishSnapshot();

        if (!terminalCallbackSent) {
            terminalCallbackSent = true;
            try {
                terminalListener.onTerminated(this, publishedSnapshot);
            } catch (RuntimeException ignored) {
                // 观察者异常不得阻止房间进入终止状态。
            }
        }
        return publishedSnapshot;
    }

    private void touch(PlayerSlot player) {
        player.lastInboundNanos = nanoTime.getAsLong();
    }

    private PlayerSlot requireCurrentSession(String playerId, String sessionId) {
        PlayerSlot player = players.get(playerId);
        if (player == null || player.state != PlayerState.CONNECTED || player.session == null
            || !player.session.sessionId().equals(sessionId)) {
            throw new RoomException("SESSION_MISMATCH", "Session is not current for this player");
        }
        return player;
    }

    private void broadcast(Envelope envelope) {
        List<PlayerSlot> failed = null;
        for (PlayerSlot player : players.values()) {
            if (player.state == PlayerState.CONNECTED && player.session != null
                && !safeSend(player.session, envelope)) {
                if (failed == null) {
                    failed = new ArrayList<>();
                }
                failed.add(player);
            }
        }
        if (failed != null && state == RoomState.ACTIVE) {
            for (PlayerSlot player : failed) {
                DataPlaneSession failedSession = player.session;
                player.session = null;
                player.state = PlayerState.RECONNECTING;
                player.connectionGeneration++;
                cancel(player.idleTask);
                safeClose(
                    failedSession,
                    SessionCloseCodes.SLOW_CONSUMER,
                    "SLOW_CONSUMER"
                );
                long generation = player.connectionGeneration;
                player.reconnectTask = executor.schedule(
                    () -> onReconnectTimeout(player, generation),
                    settings.reconnectGrace().toNanos(),
                    TimeUnit.NANOSECONDS
                );
            }
            publishSnapshot();
        }
    }

    private void sendOrDisconnect(PlayerSlot player, Envelope envelope) {
        if (player.session == null || !safeSend(player.session, envelope)) {
            if (state == RoomState.ACTIVE && player.state == PlayerState.CONNECTED) {
                DataPlaneSession failed = player.session;
                player.session = null;
                player.state = PlayerState.RECONNECTING;
                player.connectionGeneration++;
                cancel(player.idleTask);
                safeClose(failed, SessionCloseCodes.SLOW_CONSUMER, "SLOW_CONSUMER");
                long generation = player.connectionGeneration;
                player.reconnectTask = executor.schedule(
                    () -> onReconnectTimeout(player, generation),
                    settings.reconnectGrace().toNanos(),
                    TimeUnit.NANOSECONDS
                );
                publishSnapshot();
            }
        }
    }

    private boolean safeSend(DataPlaneSession session, Envelope envelope) {
        if (session == null || !session.isWritable()) {
            return false;
        }
        try {
            session.send(envelope);
            return true;
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    private static void safeClose(DataPlaneSession session, int statusCode, String reason) {
        if (session == null) {
            return;
        }
        try {
            session.close(statusCode, reason);
        } catch (RuntimeException ignored) {
            // 关闭操作尽力而为，但房间状态仍必须继续推进。
        }
    }

    private Envelope serverHello(
        String playerId,
        long replayFrom,
        long replayTo,
        String requestId
    ) {
        return Envelope.newBuilder()
            .setProtocolVersion(settings.protocolVersion())
            .setRequestId(requestId)
            .setServerHello(
                ServerHello.newBuilder()
                    .setNodeId(nodeId)
                    .setRoomId(roomId)
                    .setMatchId(matchId)
                    .setPlayerId(playerId)
                    .setMatchPhase(protocolPhase(matchPhase))
                    .setCurrentFrame((int) currentFrame)
                    .setTickRate(settings.tickRate())
                    .setInputDelayFrames(settings.inputDelayFrames())
                    .setMaxLeadFrames(settings.maxLeadFrames())
                    .setReplayFromFrame((int) replayFrom)
                    .setReplayToFrame((int) replayTo)
                    .setClientPingIntervalMs(
                        (int) settings.clientPingInterval().toMillis()
                    )
                    .setConnectionIdleTimeoutMs(
                        (int) settings.connectionIdleTimeout().toMillis()
                    )
                    .setReconnectGraceMs(
                        (int) settings.reconnectGrace().toMillis()
                    )
            )
            .build();
    }

    private Envelope matchEvent(
        EventType type,
        String playerId,
        String reason,
        long frame
    ) {
        return Envelope.newBuilder()
            .setProtocolVersion(settings.protocolVersion())
            .setMatchEvent(
                MatchEvent.newBuilder()
                    .setType(type)
                    .setPlayerId(playerId)
                    .setReason(reason)
                    .setFrameId((int) frame)
            )
            .build();
    }

    private static com.rainnov.lockstep.protocol.MatchPhase protocolPhase(MatchPhase phase) {
        return switch (phase) {
            case WAITING_FOR_PLAYERS ->
                com.rainnov.lockstep.protocol.MatchPhase.MATCH_PHASE_WAITING_FOR_PLAYERS;
            case RUNNING -> com.rainnov.lockstep.protocol.MatchPhase.MATCH_PHASE_RUNNING;
            case FINISHED -> com.rainnov.lockstep.protocol.MatchPhase.MATCH_PHASE_FINISHED;
            case NONE -> com.rainnov.lockstep.protocol.MatchPhase.MATCH_PHASE_UNSPECIFIED;
        };
    }

    private long earliestHistoryFrame() {
        if (history.isEmpty()) {
            return 0;
        }
        return Integer.toUnsignedLong(history.getFirst().getServerFrame().getFrameId());
    }

    private boolean allPlayersConnected() {
        if (players.isEmpty()) {
            return false;
        }
        for (PlayerSlot player : players.values()) {
            if (player.state != PlayerState.CONNECTED) {
                return false;
            }
        }
        return true;
    }

    private void validatePlayers(List<String> playerIds) {
        if (playerIds.isEmpty()) {
            throw new RoomException("INVALID_PLAYERS", "At least one player is required");
        }
        if (playerIds.size() > settings.maxPlayers()) {
            throw new RoomException("INVALID_PLAYERS", "Player count exceeds room capacity");
        }
        LinkedHashMap<String, Boolean> unique = new LinkedHashMap<>();
        for (String playerId : playerIds) {
            String checked = requireText(playerId, "playerId");
            if (unique.put(checked, Boolean.TRUE) != null) {
                throw new RoomException("INVALID_PLAYERS", "Player ids must be unique");
            }
        }
    }

    private AllocationSnapshot allocationSnapshot() {
        return new AllocationSnapshot(
            allocationId,
            nodeId,
            roomId,
            matchId,
            List.copyOf(players.keySet()),
            state,
            matchPhase,
            currentFrame,
            settings.protocolVersion(),
            settings.tickRate(),
            settings.inputDelayFrames(),
            settings.maxLeadFrames(),
            joinDeadline
        );
    }

    private void publishSnapshot() {
        List<PlayerSnapshot> playerSnapshots = players.values().stream()
            .map(player -> new PlayerSnapshot(
                player.playerId,
                player.state,
                player.session == null ? null : player.session.sessionId(),
                player.lastInputFrame,
                player.lastSequence
            ))
            .toList();
        publishedSnapshot = new RoomSnapshot(
            nodeId,
            roomId,
            allocationId,
            matchId,
            state,
            matchPhase,
            currentFrame,
            playerSnapshots,
            createdAt,
            activatedAt,
            startedAt,
            joinDeadline,
            terminatedAt,
            terminationMode,
            terminationReason,
            lastTickLagNanos
        );
    }

    private void requireState(RoomState expected) {
        if (state != expected) {
            throw new RoomException(
                "INVALID_ROOM_STATE",
                "Expected " + expected + " but room is " + state
            );
        }
    }

    private <T> CompletionStage<T> submit(CheckedSupplier<T> action) {
        CompletableFuture<T> future = new CompletableFuture<>();
        Runnable command = () -> {
            try {
                future.complete(action.get());
            } catch (Throwable error) {
                future.completeExceptionally(error);
            }
        };
        if (executor.inEventLoop()) {
            command.run();
        } else {
            try {
                executor.execute(command);
            } catch (RuntimeException error) {
                future.completeExceptionally(error);
            }
        }
        return future;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static int toUint32(long value, String name) {
        requireUint32(value, name);
        return (int) value;
    }

    private static void requireUint32(long value, String name) {
        if (!isUint32(value)) {
            throw new IllegalArgumentException(name + " must be an unsigned 32-bit value");
        }
    }

    private static boolean isUint32(long value) {
        return value >= 0 && value <= MAX_UINT32;
    }

    private static void cancel(ScheduledFuture<?> future) {
        if (future != null) {
            future.cancel(false);
        }
    }

    @FunctionalInterface
    private interface CheckedSupplier<T> {
        T get() throws Exception;
    }

    private static final class PlayerSlot {

        private final String playerId;
        private PlayerState state = PlayerState.RESERVED;
        private DataPlaneSession session;
        private long lastInboundNanos;
        private long lastInputFrame;
        private long lastSequence;
        private long connectionGeneration;
        private ScheduledFuture<?> idleTask;
        private ScheduledFuture<?> reconnectTask;

        private PlayerSlot(String playerId) {
            this.playerId = playerId;
        }
    }

    private record PlayerInput(long sequence, byte[] payload) {
    }
}
