package com.rainnov.lockstep.room;

import com.rainnov.lockstep.protocol.Envelope;
import com.rainnov.lockstep.protocol.EventType;
import com.rainnov.lockstep.protocol.ServerFrame;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GameRoomTest {

    private NettyRoomEventLoopProvider eventLoops;

    @AfterEach
    void tearDown() {
        if (eventLoops != null) {
            eventLoops.close();
        }
    }

    @Test
    void activatesStartsAndBuildsOrderedFramesWithNoOps() throws Exception {
        eventLoops = new NettyRoomEventLoopProvider(1);
        List<RoomSnapshot> terminal = new CopyOnWriteArrayList<>();
        GameRoom room = new GameRoom(
            "node-1",
            "room-1",
            settings(5, 16, Duration.ofSeconds(1), Duration.ofSeconds(1)),
            eventLoops.next(),
            (ignored, snapshot) -> terminal.add(snapshot)
        );

        assertThat(await(room.initialize()).state()).isEqualTo(RoomState.READY);
        AllocationSnapshot allocation =
            await(room.activate("allocation-1", "match-1", List.of("p1", "p2")));
        assertThat(allocation.roomState()).isEqualTo(RoomState.ACTIVE);
        assertThat(allocation.matchPhase()).isEqualTo(MatchPhase.WAITING_FOR_PLAYERS);

        RecordingSession first = new RecordingSession("session-1");
        RecordingSession second = new RecordingSession("session-2");
        await(room.connect("match-1", "p1", first, 0, "hello-request"));
        ConnectionSnapshot connection =
            await(room.connect("match-1", "p2", second, 0));
        assertThat(connection.matchPhase()).isEqualTo(MatchPhase.RUNNING);
        var hello = first.messages.stream()
            .filter(Envelope::hasServerHello)
            .findFirst()
            .orElseThrow()
            .getServerHello();
        assertThat(Integer.toUnsignedLong(hello.getClientPingIntervalMs())).isEqualTo(20);
        assertThat(Integer.toUnsignedLong(hello.getConnectionIdleTimeoutMs())).isEqualTo(1_000);
        assertThat(Integer.toUnsignedLong(hello.getReconnectGraceMs())).isEqualTo(1_000);
        assertThat(first.messages.stream()
            .filter(Envelope::hasServerHello)
            .findFirst()
            .orElseThrow()
            .getRequestId()).isEqualTo("hello-request");

        byte[] command = new byte[]{1, 2, 3};
        assertThat(await(room.acceptInput("p1", "session-1", 1, 1, command)).disposition())
            .isEqualTo(InputDisposition.ACCEPTED);
        assertThat(await(room.acceptInput("p1", "session-1", 1, 1, command)).disposition())
            .isEqualTo(InputDisposition.DUPLICATE_IGNORED);
        assertThat(await(room.acceptInput(
            "p1",
            "session-1",
            1,
            2,
            new byte[]{9},
            "request-conflict"
        )).disposition()).isEqualTo(InputDisposition.REJECTED_CONFLICT);
        assertThat(first.messages.stream()
            .filter(Envelope::hasProtocolError)
            .toList()
            .getLast()
            .getRequestId()).isEqualTo("request-conflict");

        waitUntil(() -> first.serverFrames().size() >= 1, Duration.ofSeconds(2));
        ServerFrame frame = first.serverFrames().getFirst();
        assertThat(Integer.toUnsignedLong(frame.getFrameId())).isEqualTo(1);
        assertThat(frame.getInputsList()).extracting(input -> input.getPlayerId())
            .containsExactly("p1", "p2");
        assertThat(frame.getInputs(0).getNoOp()).isFalse();
        assertThat(frame.getInputs(0).getPayload().toByteArray()).containsExactly(command);
        assertThat(frame.getInputs(1).getNoOp()).isTrue();

        RoomSnapshot ended = await(room.terminate(
            TerminationMode.GRACEFUL,
            TerminationReason.MATCH_COMPLETED
        ));
        assertThat(ended.state()).isEqualTo(RoomState.TERMINATED);
        assertThat(terminal).hasSize(1);
        assertThat(first.closed).isTrue();
        assertThatThrownBy(() -> await(room.initialize()))
            .hasRootCauseInstanceOf(RoomException.class);
    }

    @Test
    void reconnectTakesOverAndReplaysFramesInOrder() throws Exception {
        eventLoops = new NettyRoomEventLoopProvider(1);
        GameRoom room = room(settings(
            40,
            32,
            Duration.ofSeconds(1),
            Duration.ofSeconds(1)
        ));
        RecordingSession old = new RecordingSession("old");
        await(room.connect("match-1", "p1", old, 0));
        waitUntil(() -> old.serverFrames().size() >= 4, Duration.ofSeconds(2));

        long applied = Integer.toUnsignedLong(old.serverFrames().get(1).getFrameId());
        RecordingSession replacement = new RecordingSession("replacement");
        ConnectionSnapshot connection =
            await(room.connect("match-1", "p1", replacement, applied));

        assertThat(connection.takeover()).isTrue();
        assertThat(connection.replayFromFrame()).isEqualTo(applied + 1);
        assertThat(connection.replayToFrame()).isGreaterThanOrEqualTo(applied + 2);
        assertThat(old.closed).isTrue();
        assertThat(old.closeCode).isEqualTo(SessionCloseCodes.SESSION_REPLACED);

        List<Long> replayed = replacement.serverFrames().stream()
            .map(frame -> Integer.toUnsignedLong(frame.getFrameId()))
            .toList();
        assertThat(replayed).isNotEmpty();
        assertThat(replayed.getFirst()).isEqualTo(applied + 1);
        assertThat(replayed).isSorted();
        assertThat(replacement.events()).anyMatch(event ->
            event.getType() == EventType.EVENT_TYPE_CATCH_UP_COMPLETED
        );
        await(room.terminate(TerminationMode.FORCE, TerminationReason.MATCH_COMPLETED));
    }

    @Test
    void expiredReplayTerminatesTheSingleUseRoom() throws Exception {
        eventLoops = new NettyRoomEventLoopProvider(1);
        List<RoomSnapshot> terminal = new CopyOnWriteArrayList<>();
        GameRoom room = new GameRoom(
            "node-1",
            "room-1",
            settings(60, 2, Duration.ofSeconds(1), Duration.ofSeconds(1)),
            eventLoops.next(),
            (ignored, snapshot) -> terminal.add(snapshot)
        );
        await(room.initialize());
        await(room.activate("allocation-1", "match-1", List.of("p1")));
        RecordingSession old = new RecordingSession("old");
        await(room.connect("match-1", "p1", old, 0));
        waitUntil(() -> old.serverFrames().size() >= 4, Duration.ofSeconds(2));
        await(room.disconnect("p1", "old", "NETWORK_LOST"));

        RecordingSession replacement = new RecordingSession("replacement");
        assertThatThrownBy(() ->
            await(room.connect("match-1", "p1", replacement, 0))
        ).hasRootCauseInstanceOf(RoomException.class);
        assertThat(room.cachedSnapshot().terminationReason())
            .isEqualTo(TerminationReason.REPLAY_HISTORY_EXPIRED);
        assertThat(terminal).hasSize(1);
    }

    @Test
    void heartbeatIdleThenReconnectTimeoutEndsMatch() throws Exception {
        eventLoops = new NettyRoomEventLoopProvider(1);
        GameRoom room = room(settings(
            20,
            16,
            Duration.ofMillis(80),
            Duration.ofMillis(90)
        ));
        RecordingSession session = new RecordingSession("session");
        await(room.connect("match-1", "p1", session, 0));

        waitUntil(
            () -> room.cachedSnapshot().players().getFirst().state()
                == PlayerState.RECONNECTING,
            Duration.ofSeconds(1)
        );
        assertThat(session.closeCode).isEqualTo(SessionCloseCodes.HEARTBEAT_TIMEOUT);
        waitUntil(
            () -> room.cachedSnapshot().state() == RoomState.TERMINATED,
            Duration.ofSeconds(1)
        );
        assertThat(room.cachedSnapshot().terminationReason())
            .isEqualTo(TerminationReason.RECONNECT_TIMEOUT);
    }

    private GameRoom room(RoomSettings settings) throws Exception {
        GameRoom room = new GameRoom(
            "node-1",
            "room-1",
            settings,
            eventLoops.next(),
            (ignored, snapshot) -> {
            }
        );
        await(room.initialize());
        await(room.activate("allocation-1", "match-1", List.of("p1")));
        return room;
    }

    private static RoomSettings settings(
        int tickRate,
        int historyFrames,
        Duration idleTimeout,
        Duration reconnectGrace
    ) {
        return new RoomSettings(
            1,
            8,
            tickRate,
            2,
            4,
            Duration.ofSeconds(2),
            Duration.ofMillis(20),
            idleTimeout,
            reconnectGrace,
            Duration.ofSeconds(5),
            historyFrames,
            1024
        );
    }

    private static <T> T await(java.util.concurrent.CompletionStage<T> stage)
        throws Exception {
        return stage.toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    private static void waitUntil(CheckedBoolean condition, Duration timeout)
        throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("Condition did not become true before timeout");
            }
            Thread.sleep(5);
        }
    }

    @FunctionalInterface
    private interface CheckedBoolean {
        boolean getAsBoolean() throws Exception;
    }

    private static final class RecordingSession implements DataPlaneSession {

        private final String id;
        private final List<Envelope> messages = new CopyOnWriteArrayList<>();
        private volatile boolean writable = true;
        private volatile boolean closed;
        private volatile int closeCode;

        private RecordingSession(String id) {
            this.id = id;
        }

        @Override
        public String sessionId() {
            return id;
        }

        @Override
        public boolean isWritable() {
            return writable && !closed;
        }

        @Override
        public void send(Envelope envelope) {
            messages.add(envelope);
        }

        @Override
        public void close(int statusCode, String reason) {
            closeCode = statusCode;
            closed = true;
            writable = false;
        }

        private List<ServerFrame> serverFrames() {
            return messages.stream()
                .filter(Envelope::hasServerFrame)
                .map(Envelope::getServerFrame)
                .toList();
        }

        private List<com.rainnov.lockstep.protocol.MatchEvent> events() {
            return messages.stream()
                .filter(Envelope::hasMatchEvent)
                .map(Envelope::getMatchEvent)
                .toList();
        }
    }
}
