package com.rainnov.lockstep.room;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 协调由一次性 {@link GameRoom} 实例组成的固定大小房间池。
 * 房间池索引仅由单个协调器 {@link EventExecutor} 修改。
 */
public final class RoomPoolManager implements AutoCloseable {

    private final String nodeId;
    private final int targetCapacity;
    private final RoomSettings roomSettings;
    private final RoomEventLoopProvider eventLoops;
    private final DefaultEventExecutor coordinator;
    private final Duration tombstoneRetention;
    private final Duration healthCheckInterval;
    private final Duration hardCleanupTimeout;
    private final int healthFailureThreshold;
    private final Clock clock;
    private final Supplier<String> roomIdSupplier;
    private final Supplier<String> allocationIdSupplier;
    private final LinkedHashMap<String, GameRoom> rooms = new LinkedHashMap<>();
    private final ArrayDeque<String> readyRoomIds = new ArrayDeque<>();
    private final Map<String, String> matchToRoom = new HashMap<>();
    private final Set<String> pendingMatches = new LinkedHashSet<>();
    private final Map<String, PendingActivation> pendingActivations = new HashMap<>();
    private final Map<String, Tombstone> tombstones = new LinkedHashMap<>();
    private final Map<String, HealthProbeTracker> healthProbes = new HashMap<>();
    private final List<Consumer<RoomSnapshot>> terminationListeners =
        new CopyOnWriteArrayList<>();
    private final List<Consumer<TerminationReason>> fatalNodeFailureListeners =
        new CopyOnWriteArrayList<>();
    private final List<Runnable> roomCreationFailureListeners =
        new CopyOnWriteArrayList<>();

    private boolean started;
    private boolean acceptingAllocations;
    private boolean closed;
    private int roomsBeingCreated;
    private CompletableFuture<CapacitySnapshot> startFuture;
    private CompletableFuture<Void> drainFuture;
    private ScheduledFuture<?> healthTask;
    private ScheduledFuture<?> forcedDrainTask;
    private ScheduledFuture<?> hardCleanupTask;
    private ScheduledFuture<?> capacityReconcileTask;
    private TerminationReason drainReason = TerminationReason.NODE_DRAINING;
    private int consecutiveCreationFailures;

    public RoomPoolManager(
        String nodeId,
        int targetCapacity,
        RoomSettings roomSettings,
        RoomEventLoopProvider eventLoops,
        Duration tombstoneRetention,
        Duration healthCheckInterval,
        int healthFailureThreshold
    ) {
        this(
            nodeId,
            targetCapacity,
            roomSettings,
            eventLoops,
            tombstoneRetention,
            healthCheckInterval,
            healthFailureThreshold,
            Clock.systemUTC(),
            () -> "room-" + UUID.randomUUID(),
            () -> "alloc-" + UUID.randomUUID()
        );
    }

    RoomPoolManager(
        String nodeId,
        int targetCapacity,
        RoomSettings roomSettings,
        RoomEventLoopProvider eventLoops,
        Duration tombstoneRetention,
        Duration healthCheckInterval,
        int healthFailureThreshold,
        Clock clock,
        Supplier<String> roomIdSupplier,
        Supplier<String> allocationIdSupplier
    ) {
        this.nodeId = requireText(nodeId, "nodeId");
        if (targetCapacity <= 0) {
            throw new IllegalArgumentException("targetCapacity must be positive");
        }
        this.targetCapacity = targetCapacity;
        this.roomSettings = Objects.requireNonNull(roomSettings, "roomSettings");
        this.eventLoops = Objects.requireNonNull(eventLoops, "eventLoops");
        this.coordinator = new DefaultEventExecutor(
            new DefaultThreadFactory("lockstep-room-coordinator", true)
        );
        this.tombstoneRetention = positive(tombstoneRetention, "tombstoneRetention");
        this.healthCheckInterval = positive(healthCheckInterval, "healthCheckInterval");
        this.hardCleanupTimeout = minimum(
            this.healthCheckInterval,
            Duration.ofSeconds(1)
        );
        if (healthFailureThreshold <= 0) {
            throw new IllegalArgumentException("healthFailureThreshold must be positive");
        }
        this.healthFailureThreshold = healthFailureThreshold;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.roomIdSupplier = Objects.requireNonNull(roomIdSupplier, "roomIdSupplier");
        this.allocationIdSupplier =
            Objects.requireNonNull(allocationIdSupplier, "allocationIdSupplier");
    }

    public CompletionStage<CapacitySnapshot> start() {
        CompletableFuture<CapacitySnapshot> result = new CompletableFuture<>();
        executeCoordinator(() -> {
            if (closed) {
                result.completeExceptionally(
                    new RoomException("POOL_CLOSED", "Room pool is closed")
                );
                return;
            }
            if (started) {
                if (startFuture != null) {
                    startFuture.whenComplete(copyCompletion(result));
                } else {
                    result.complete(capacityOnLoop());
                }
                return;
            }
            started = true;
            acceptingAllocations = true;
            startFuture = result;
            reconcileCapacity();
            scheduleHealthSweep();
            completeStartIfReady();
        }, result);
        return result;
    }

    public CompletionStage<AllocationSnapshot> allocate(
        String matchId,
        List<String> orderedPlayerIds
    ) {
        String checkedMatchId = requireText(matchId, "matchId");
        List<String> playerIds = validatePlayerIds(orderedPlayerIds);
        CompletableFuture<AllocationSnapshot> result = new CompletableFuture<>();
        executeCoordinator(() -> {
            purgeExpiredTombstones();
            if (!started) {
                result.completeExceptionally(
                    new RoomException("POOL_NOT_STARTED", "Room pool is not started")
                );
                return;
            }
            if (!acceptingAllocations) {
                result.completeExceptionally(
                    new RoomException("NODE_DRAINING", "Node is not accepting allocations")
                );
                return;
            }
            if (matchToRoom.containsKey(checkedMatchId)
                || pendingMatches.contains(checkedMatchId)) {
                result.completeExceptionally(
                    new RoomException(
                        "MATCH_ALREADY_ALLOCATED",
                        "Match is already allocated on this node"
                    )
                );
                return;
            }

            GameRoom room = pollReadyRoom();
            if (room == null) {
                result.completeExceptionally(
                    new RoomException(
                        "ROOM_CAPACITY_EXHAUSTED",
                        "No ready room is currently available"
                    )
                );
                return;
            }

            String allocationId = uniqueId(allocationIdSupplier, "allocation");
            pendingMatches.add(checkedMatchId);
            pendingActivations.put(
                room.roomId(),
                new PendingActivation(checkedMatchId, result)
            );
            room.activate(allocationId, checkedMatchId, playerIds)
                .whenComplete((allocation, error) -> executeCoordinator(() -> {
                    PendingActivation pending = pendingActivations.remove(room.roomId());
                    if (pending == null) {
                        return;
                    }
                    pendingMatches.remove(pending.matchId);
                    if (error != null) {
                        if (room.state() == RoomState.READY && acceptingAllocations) {
                            readyRoomIds.addLast(room.roomId());
                        } else if (room.state() != RoomState.TERMINATED) {
                            room.fail(TerminationReason.ACTIVATION_FAILED);
                        }
                        result.completeExceptionally(unwrap(error));
                        return;
                    }
                    if (!acceptingAllocations || rooms.get(room.roomId()) != room) {
                        room.terminate(TerminationMode.FORCE, drainReason);
                        result.completeExceptionally(
                            new RoomException(
                                "NODE_DRAINING",
                                "Node started draining before room activation completed"
                            )
                        );
                        return;
                    }
                    matchToRoom.put(checkedMatchId, room.roomId());
                    result.complete(allocation);
                }, result));
        }, result);
        return result;
    }

    public CompletionStage<ConnectionSnapshot> connect(
        String roomId,
        String matchId,
        String playerId,
        DataPlaneSession session,
        long lastAppliedFrame
    ) {
        return connect(roomId, matchId, playerId, session, lastAppliedFrame, "");
    }

    public CompletionStage<ConnectionSnapshot> connect(
        String roomId,
        String matchId,
        String playerId,
        DataPlaneSession session,
        long lastAppliedFrame,
        String requestId
    ) {
        return withLiveRoom(
            roomId,
            room -> room.connect(
                matchId,
                playerId,
                session,
                lastAppliedFrame,
                requestId
            )
        );
    }

    public CompletionStage<InputResult> acceptInput(
        String roomId,
        String playerId,
        String sessionId,
        long targetFrame,
        long sequence,
        byte[] payload
    ) {
        return acceptInput(
            roomId,
            playerId,
            sessionId,
            targetFrame,
            sequence,
            payload,
            ""
        );
    }

    public CompletionStage<InputResult> acceptInput(
        String roomId,
        String playerId,
        String sessionId,
        long targetFrame,
        long sequence,
        byte[] payload,
        String requestId
    ) {
        return withLiveRoom(
            roomId,
            room -> room.acceptInput(
                playerId,
                sessionId,
                targetFrame,
                sequence,
                payload,
                requestId
            )
        );
    }

    public CompletionStage<Void> acceptPing(
        String roomId,
        String playerId,
        String sessionId,
        long sequence,
        String requestId
    ) {
        return withLiveRoom(
            roomId,
            room -> room.acceptPing(playerId, sessionId, sequence, requestId)
        );
    }

    public CompletionStage<Void> disconnect(
        String roomId,
        String playerId,
        String sessionId,
        String reason
    ) {
        return withLiveRoom(
            roomId,
            room -> room.disconnect(playerId, sessionId, reason)
        );
    }

    public CompletionStage<RoomSnapshot> roomSnapshot(String roomId) {
        String checkedRoomId = requireText(roomId, "roomId");
        CompletableFuture<RoomSnapshot> result = new CompletableFuture<>();
        executeCoordinator(() -> {
            purgeExpiredTombstones();
            GameRoom room = rooms.get(checkedRoomId);
            if (room != null) {
                forward(room.snapshot(), result);
                return;
            }
            Tombstone tombstone = tombstones.get(checkedRoomId);
            if (tombstone != null) {
                result.complete(tombstone.snapshot);
                return;
            }
            result.completeExceptionally(
                new RoomException("ROOM_NOT_FOUND", "Room does not exist")
            );
        }, result);
        return result;
    }

    public CompletionStage<RoomSnapshot> terminate(
        String roomId,
        String expectedMatchId,
        TerminationMode mode,
        TerminationReason reason
    ) {
        String checkedRoomId = requireText(roomId, "roomId");
        String checkedMatchId = requireText(expectedMatchId, "matchId");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(reason, "reason");
        CompletableFuture<RoomSnapshot> result = new CompletableFuture<>();
        executeCoordinator(() -> {
            purgeExpiredTombstones();
            GameRoom room = rooms.get(checkedRoomId);
            if (room != null) {
                if (!Objects.equals(checkedMatchId, room.cachedSnapshot().matchId())) {
                    result.completeExceptionally(
                        new RoomException("MATCH_MISMATCH", "Match does not belong to room")
                    );
                    return;
                }
                forward(room.terminate(mode, reason), result);
                return;
            }
            Tombstone tombstone = tombstones.get(checkedRoomId);
            if (tombstone != null) {
                if (!Objects.equals(checkedMatchId, tombstone.snapshot.matchId())) {
                    result.completeExceptionally(
                        new RoomException(
                            "MATCH_MISMATCH",
                            "Match does not belong to room"
                        )
                    );
                } else {
                    result.complete(tombstone.snapshot);
                }
                return;
            }
            result.completeExceptionally(
                new RoomException("ROOM_NOT_FOUND", "Room does not exist")
            );
        }, result);
        return result;
    }

    public CompletionStage<CapacitySnapshot> capacity() {
        CompletableFuture<CapacitySnapshot> result = new CompletableFuture<>();
        executeCoordinator(() -> {
            purgeExpiredTombstones();
            result.complete(capacityOnLoop());
        }, result);
        return result;
    }

    public CompletionStage<List<RoomSnapshot>> liveSnapshots() {
        CompletableFuture<List<RoomSnapshot>> result = new CompletableFuture<>();
        executeCoordinator(() -> result.complete(
            rooms.values().stream()
                .map(GameRoom::cachedSnapshot)
                .toList()
        ), result);
        return result;
    }

    public CompletionStage<Void> drain(Duration gracePeriod) {
        Objects.requireNonNull(gracePeriod, "gracePeriod");
        if (gracePeriod.isNegative()) {
            throw new IllegalArgumentException("gracePeriod must not be negative");
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        executeCoordinator(() -> {
            if (drainFuture != null) {
                drainFuture.whenComplete(copyCompletion(result));
                return;
            }
            beginDrainOnLoop(
                gracePeriod,
                TerminationReason.NODE_DRAINING,
                result
            );
        }, result);
        return result;
    }

    public void addTerminationListener(Consumer<RoomSnapshot> listener) {
        terminationListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    public void addFatalNodeFailureListener(Consumer<TerminationReason> listener) {
        fatalNodeFailureListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    public void addRoomCreationFailureListener(Runnable listener) {
        roomCreationFailureListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        if (coordinator.inEventLoop()) {
            throw new IllegalStateException("RoomPoolManager.close must not run on coordinator");
        }
        try {
            long waitMillis = Math.max(
                100,
                hardCleanupTimeout.plusSeconds(2).toMillis()
            );
            drain(Duration.ZERO).toCompletableFuture().get(waitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException | TimeoutException ignored) {
            // 即使外部执行器无响应，关闭过程也必须在有限时间内完成。
        } finally {
            closed = true;
            coordinator.shutdownGracefully(
                0,
                Math.max(1, hardCleanupTimeout.toNanos()),
                TimeUnit.NANOSECONDS
            );
            try {
                coordinator.awaitTermination(
                    Math.max(100, hardCleanupTimeout.plusSeconds(1).toMillis()),
                    TimeUnit.MILLISECONDS
                );
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            eventLoops.close();
        }
    }

    private <T> CompletionStage<T> withLiveRoom(
        String roomId,
        Function<GameRoom, CompletionStage<T>> command
    ) {
        String checkedRoomId = requireText(roomId, "roomId");
        CompletableFuture<T> result = new CompletableFuture<>();
        executeCoordinator(() -> {
            GameRoom room = rooms.get(checkedRoomId);
            if (room == null) {
                result.completeExceptionally(
                    new RoomException("ROOM_NOT_FOUND", "Room does not exist")
                );
                return;
            }
            try {
                forward(command.apply(room), result);
            } catch (Throwable error) {
                result.completeExceptionally(error);
            }
        }, result);
        return result;
    }

    private void reconcileCapacity() {
        if (!acceptingAllocations || closed) {
            return;
        }
        int missing = targetCapacity - rooms.size();
        for (int index = 0; index < missing && acceptingAllocations; index++) {
            createRoom();
        }
    }

    private void createRoom() {
        roomsBeingCreated++;
        String roomId = uniqueId(roomIdSupplier, "room");
        GameRoom room = new GameRoom(
            nodeId,
            roomId,
            roomSettings,
            eventLoops.next(),
            this::onRoomTerminated
        );
        rooms.put(roomId, room);
        room.initialize().whenComplete((snapshot, error) -> executeCoordinator(() -> {
            roomsBeingCreated--;
            if (error != null) {
                rooms.remove(roomId, room);
                for (Runnable listener : roomCreationFailureListeners) {
                    try {
                        listener.run();
                    } catch (RuntimeException ignored) {
                        // 指标记录异常不得阻止限时补池处理。
                    }
                }
                if (acceptingAllocations) {
                    consecutiveCreationFailures++;
                    if (consecutiveCreationFailures >= healthFailureThreshold) {
                        beginFatalHealthDrain();
                    } else {
                        scheduleCapacityReconcile();
                    }
                }
                completeStartIfReady();
                return;
            }
            consecutiveCreationFailures = 0;
            if (acceptingAllocations && rooms.get(roomId) == room) {
                readyRoomIds.addLast(roomId);
            } else {
                room.terminate(TerminationMode.FORCE, TerminationReason.NODE_DRAINING);
            }
            completeStartIfReady();
        }, startFuture));
    }

    private void scheduleCapacityReconcile() {
        if (capacityReconcileTask != null || !acceptingAllocations || closed) {
            return;
        }
        capacityReconcileTask = coordinator.schedule(() -> {
            capacityReconcileTask = null;
            reconcileCapacity();
        }, hardCleanupTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void completeStartIfReady() {
        if (startFuture != null && !startFuture.isDone()
            && roomsBeingCreated == 0
            && rooms.size() == targetCapacity
            && readyRoomIds.size() == targetCapacity) {
            startFuture.complete(capacityOnLoop());
        }
    }

    private GameRoom pollReadyRoom() {
        while (!readyRoomIds.isEmpty()) {
            String roomId = readyRoomIds.removeFirst();
            GameRoom room = rooms.get(roomId);
            if (room != null && room.state() == RoomState.READY) {
                return room;
            }
        }
        return null;
    }

    private void onRoomTerminated(GameRoom room, RoomSnapshot snapshot) {
        executeCoordinator(() -> removeRoomOnLoop(room, snapshot), null);
    }

    private void scheduleHealthSweep() {
        cancel(healthTask);
        healthTask = coordinator.schedule(
            this::runHealthSweep,
            healthCheckInterval.toNanos(),
            TimeUnit.NANOSECONDS
        );
    }

    private void runHealthSweep() {
        if (!acceptingAllocations || closed) {
            return;
        }
        purgeExpiredTombstones();
        if (!checkAndRepairIndexes()) {
            return;
        }
        List<GameRoom> snapshot = new ArrayList<>(rooms.values());
        for (GameRoom room : snapshot) {
            startHealthProbe(room);
        }
        scheduleHealthSweep();
    }

    private void startHealthProbe(GameRoom room) {
        if (rooms.get(room.roomId()) != room) {
            return;
        }
        HealthProbeTracker tracker =
            healthProbes.computeIfAbsent(room.roomId(), ignored -> new HealthProbeTracker());
        if (tracker.inFlight || tracker.quarantined) {
            return;
        }
        tracker.inFlight = true;
        long generation = ++tracker.generation;
        tracker.deadlineTask = coordinator.schedule(
            () -> completeHealthProbe(room, generation, ProbeOutcome.TIMEOUT),
            healthCheckInterval.toNanos(),
            TimeUnit.NANOSECONDS
        );
        CompletionStage<Boolean> probe;
        try {
            probe = room.healthCheck();
        } catch (Throwable error) {
            completeHealthProbe(room, generation, ProbeOutcome.UNHEALTHY);
            return;
        }
        probe.whenComplete((healthy, error) -> executeCoordinator(
            () -> completeHealthProbe(
                room,
                generation,
                error == null && Boolean.TRUE.equals(healthy)
                    ? ProbeOutcome.HEALTHY
                    : ProbeOutcome.UNHEALTHY
            ),
            null
        ));
    }

    private void completeHealthProbe(
        GameRoom room,
        long generation,
        ProbeOutcome outcome
    ) {
        if (rooms.get(room.roomId()) != room || !acceptingAllocations) {
            return;
        }
        HealthProbeTracker tracker = healthProbes.get(room.roomId());
        if (tracker == null || !tracker.inFlight || tracker.generation != generation) {
            return;
        }
        tracker.inFlight = false;
        cancel(tracker.deadlineTask);
        tracker.deadlineTask = null;
        if (outcome == ProbeOutcome.TIMEOUT) {
            beginFatalHealthDrain();
            return;
        }
        if (outcome == ProbeOutcome.HEALTHY) {
            tracker.consecutiveFailures = 0;
            return;
        }

        tracker.consecutiveFailures++;
        if (tracker.consecutiveFailures < healthFailureThreshold) {
            return;
        }
        tracker.quarantined = true;
        CompletionStage<RoomSnapshot> failure;
        try {
            failure = room.fail(TerminationReason.HEALTH_CHECK_FAILED);
        } catch (Throwable error) {
            beginFatalHealthDrain();
            return;
        }
        tracker.failureCleanupTask = coordinator.schedule(
            this::beginFatalHealthDrain,
            hardCleanupTimeout.toNanos(),
            TimeUnit.NANOSECONDS
        );
        failure.whenComplete((ignored, error) -> {
            if (error != null) {
                executeCoordinator(
                    this::beginFatalHealthDrain,
                    null
                );
            }
        });
    }

    private int healthFailureCount(String roomId) {
        HealthProbeTracker tracker = healthProbes.get(roomId);
        return tracker == null ? 0 : tracker.consecutiveFailures;
    }

    private void cancelHealthProbes() {
        for (HealthProbeTracker tracker : healthProbes.values()) {
            tracker.generation++;
            tracker.inFlight = false;
            cancel(tracker.deadlineTask);
            cancel(tracker.failureCleanupTask);
            tracker.deadlineTask = null;
            tracker.failureCleanupTask = null;
        }
    }

    private boolean checkAndRepairIndexes() {
        if (rooms.size() > targetCapacity) {
            beginFatalHealthDrain();
            return false;
        }

        LinkedHashSet<String> expectedReady = new LinkedHashSet<>();
        LinkedHashSet<String> expectedPendingMatches = new LinkedHashSet<>();
        HashMap<String, String> expectedMatchToRoom = new HashMap<>();
        for (Map.Entry<String, GameRoom> entry : rooms.entrySet()) {
            String indexedRoomId = entry.getKey();
            GameRoom room = entry.getValue();
            if (!indexedRoomId.equals(room.roomId())) {
                beginFatalHealthDrain();
                return false;
            }

            PendingActivation pending = pendingActivations.get(indexedRoomId);
            if (pending != null) {
                if (!expectedPendingMatches.add(pending.matchId())) {
                    beginFatalHealthDrain();
                    return false;
                }
            } else {
                String matchId = room.cachedSnapshot().matchId();
                if (matchId != null
                    && expectedMatchToRoom.put(matchId, indexedRoomId) != null) {
                    beginFatalHealthDrain();
                    return false;
                }
                if (room.state() == RoomState.READY) {
                    expectedReady.add(indexedRoomId);
                }
            }
        }

        List<String> danglingActivations = pendingActivations.keySet().stream()
            .filter(roomId -> !rooms.containsKey(roomId))
            .toList();
        for (String roomId : danglingActivations) {
            PendingActivation pending = pendingActivations.remove(roomId);
            if (pending != null) {
                pending.result().completeExceptionally(
                    new RoomException(
                        "INTERNAL_ERROR",
                        "Pending allocation lost its room"
                    )
                );
            }
        }

        readyRoomIds.clear();
        readyRoomIds.addAll(expectedReady);
        pendingMatches.clear();
        pendingMatches.addAll(expectedPendingMatches);
        matchToRoom.clear();
        matchToRoom.putAll(expectedMatchToRoom);
        if (rooms.size() < targetCapacity) {
            reconcileCapacity();
        }
        return acceptingAllocations;
    }

    private void beginFatalHealthDrain() {
        if (drainFuture != null || !acceptingAllocations) {
            return;
        }
        beginDrainOnLoop(
            Duration.ZERO,
            TerminationReason.HEALTH_CHECK_FAILED,
            new CompletableFuture<>()
        );
        for (Consumer<TerminationReason> listener : fatalNodeFailureListeners) {
            try {
                listener.accept(TerminationReason.HEALTH_CHECK_FAILED);
            } catch (RuntimeException ignored) {
                // 节点状态观察者异常不得阻止限时内部排空。
            }
        }
    }

    private void beginDrainOnLoop(
        Duration gracePeriod,
        TerminationReason reason,
        CompletableFuture<Void> completion
    ) {
        acceptingAllocations = false;
        drainReason = reason;
        drainFuture = completion;
        readyRoomIds.clear();
        cancel(healthTask);
        healthTask = null;
        cancel(capacityReconcileTask);
        capacityReconcileTask = null;
        cancelHealthProbes();
        if (startFuture != null && !startFuture.isDone()) {
            startFuture.completeExceptionally(
                new RoomException("NODE_DRAINING", "Node started draining during startup")
            );
        }

        List<GameRoom> unusedRooms = rooms.values().stream()
            .filter(room -> {
                RoomState roomState = room.state();
                return !pendingActivations.containsKey(room.roomId())
                    && (roomState == RoomState.READY
                    || roomState == RoomState.INITIALIZING);
            })
            .toList();
        for (GameRoom room : unusedRooms) {
            room.terminate(TerminationMode.FORCE, reason);
        }
        if (rooms.isEmpty()) {
            completion.complete(null);
            return;
        }
        forcedDrainTask = coordinator.schedule(
            this::forceDrainOnLoop,
            gracePeriod.toNanos(),
            TimeUnit.NANOSECONDS
        );
    }

    private void forceDrainOnLoop() {
        forcedDrainTask = null;
        for (GameRoom room : List.copyOf(rooms.values())) {
            room.terminate(TerminationMode.FORCE, drainReason);
        }
        if (rooms.isEmpty() && drainFuture != null) {
            drainFuture.complete(null);
            return;
        }
        hardCleanupTask = coordinator.schedule(
            this::hardCleanupDrainOnLoop,
            hardCleanupTimeout.toNanos(),
            TimeUnit.NANOSECONDS
        );
    }

    private void hardCleanupDrainOnLoop() {
        hardCleanupTask = null;
        for (GameRoom room : List.copyOf(rooms.values())) {
            hardEvictOnLoop(room, drainReason);
        }
        if (drainFuture != null && !drainFuture.isDone()) {
            drainFuture.complete(null);
        }
    }

    private void hardEvictOnLoop(GameRoom room, TerminationReason reason) {
        if (rooms.get(room.roomId()) != room) {
            return;
        }
        removeRoomOnLoop(room, syntheticTerminalSnapshot(room, reason));
    }

    private void removeRoomOnLoop(GameRoom room, RoomSnapshot snapshot) {
        if (!rooms.remove(room.roomId(), room)) {
            return;
        }
        readyRoomIds.remove(room.roomId());
        HealthProbeTracker tracker = healthProbes.remove(room.roomId());
        if (tracker != null) {
            tracker.generation++;
            cancel(tracker.deadlineTask);
            cancel(tracker.failureCleanupTask);
        }
        PendingActivation pendingActivation = pendingActivations.remove(room.roomId());
        if (pendingActivation != null) {
            pendingMatches.remove(pendingActivation.matchId);
            pendingActivation.result.completeExceptionally(
                new RoomException(
                    !acceptingAllocations
                        ? "NODE_DRAINING"
                        : "ROOM_ACTIVATION_TERMINATED",
                    "Room terminated before activation completed"
                )
            );
        }
        if (snapshot.matchId() != null) {
            matchToRoom.remove(snapshot.matchId(), room.roomId());
        }
        tombstones.put(
            room.roomId(),
            new Tombstone(snapshot, clock.instant().plus(tombstoneRetention))
        );
        for (Consumer<RoomSnapshot> listener : terminationListeners) {
            try {
                listener.accept(snapshot);
            } catch (RuntimeException ignored) {
                // 观察者异常不得阻止房间销毁和补位。
            }
        }
        if (acceptingAllocations) {
            reconcileCapacity();
        } else if (drainFuture != null && rooms.isEmpty()) {
            cancel(forcedDrainTask);
            cancel(hardCleanupTask);
            forcedDrainTask = null;
            hardCleanupTask = null;
            drainFuture.complete(null);
        }
    }

    private RoomSnapshot syntheticTerminalSnapshot(
        GameRoom room,
        TerminationReason reason
    ) {
        RoomSnapshot source = room.cachedSnapshot();
        List<PlayerSnapshot> terminalPlayers = source.players().stream()
            .map(player -> new PlayerSnapshot(
                player.playerId(),
                player.state() == PlayerState.TIMED_OUT
                    ? PlayerState.TIMED_OUT
                    : PlayerState.COMPLETED,
                null,
                player.lastInputFrame(),
                player.lastSequence()
            ))
            .toList();
        return new RoomSnapshot(
            source.nodeId(),
            source.roomId(),
            source.allocationId(),
            source.matchId(),
            RoomState.TERMINATED,
            MatchPhase.FINISHED,
            source.currentFrame(),
            terminalPlayers,
            source.createdAt(),
            source.activatedAt(),
            source.startedAt(),
            source.joinDeadline(),
            clock.instant(),
            TerminationMode.FORCE,
            reason,
            source.lastTickLagNanos()
        );
    }

    private CapacitySnapshot capacityOnLoop() {
        int initializing = 0;
        int ready = 0;
        int activating = 0;
        int active = 0;
        int failed = 0;
        int terminating = 0;
        int healthy = 0;
        for (GameRoom room : rooms.values()) {
            RoomState state = room.state();
            switch (state) {
                case INITIALIZING -> initializing++;
                case READY -> {
                    if (pendingActivations.containsKey(room.roomId())) {
                        activating++;
                    } else {
                        ready++;
                    }
                }
                case ACTIVATING -> activating++;
                case ACTIVE -> active++;
                case FAILED -> failed++;
                case TERMINATING -> terminating++;
                case TERMINATED -> {
                    // 已终止房间将在协调器下一轮处理时移除。
                }
            }
            if (state != RoomState.FAILED && state != RoomState.TERMINATED
                && healthFailureCount(room.roomId()) < healthFailureThreshold) {
                healthy++;
            }
        }
        return new CapacitySnapshot(
            nodeId,
            acceptingAllocations,
            targetCapacity,
            initializing,
            ready,
            activating,
            active,
            failed,
            terminating,
            healthy,
            rooms.size(),
            clock.instant()
        );
    }

    private void purgeExpiredTombstones() {
        Instant now = clock.instant();
        tombstones.entrySet().removeIf(entry -> !entry.getValue().expiresAt.isAfter(now));
    }

    private List<String> validatePlayerIds(List<String> playerIds) {
        Objects.requireNonNull(playerIds, "playerIds");
        List<String> copy = List.copyOf(playerIds);
        if (copy.isEmpty() || copy.size() > roomSettings.maxPlayers()) {
            throw new RoomException(
                "INVALID_PLAYERS",
                "Player count must be between 1 and " + roomSettings.maxPlayers()
            );
        }
        Set<String> unique = new LinkedHashSet<>();
        for (String playerId : copy) {
            String checked = requireText(playerId, "playerId");
            if (!unique.add(checked)) {
                throw new RoomException("INVALID_PLAYERS", "Player ids must be unique");
            }
        }
        return copy;
    }

    private String uniqueId(Supplier<String> supplier, String kind) {
        for (int attempt = 0; attempt < 100; attempt++) {
            String value = requireText(supplier.get(), kind + "Id");
            boolean unavailable = "room".equals(kind)
                ? rooms.containsKey(value) || tombstones.containsKey(value)
                : false;
            if (!unavailable) {
                return value;
            }
        }
        throw new IllegalStateException("Could not generate a unique " + kind + " id");
    }

    private void executeCoordinator(Runnable action, CompletableFuture<?> failureTarget) {
        Runnable guarded = () -> {
            try {
                action.run();
            } catch (Throwable error) {
                if (failureTarget != null) {
                    failureTarget.completeExceptionally(error);
                }
            }
        };
        if (coordinator.inEventLoop()) {
            guarded.run();
            return;
        }
        try {
            coordinator.execute(guarded);
        } catch (RuntimeException error) {
            if (failureTarget != null) {
                failureTarget.completeExceptionally(error);
            }
        }
    }

    private static <T> void forward(
        CompletionStage<T> source,
        CompletableFuture<T> target
    ) {
        source.whenComplete((value, error) -> {
            if (error == null) {
                target.complete(value);
            } else {
                target.completeExceptionally(unwrap(error));
            }
        });
    }

    private static <T> java.util.function.BiConsumer<T, Throwable> copyCompletion(
        CompletableFuture<T> target
    ) {
        return (value, error) -> {
            if (error == null) {
                target.complete(value);
            } else {
                target.completeExceptionally(unwrap(error));
            }
        };
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof java.util.concurrent.CompletionException
            || current instanceof java.util.concurrent.ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static Duration minimum(Duration first, Duration second) {
        return first.compareTo(second) <= 0 ? first : second;
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

    private static final class HealthProbeTracker {

        private long generation;
        private int consecutiveFailures;
        private boolean inFlight;
        private boolean quarantined;
        private ScheduledFuture<?> deadlineTask;
        private ScheduledFuture<?> failureCleanupTask;
    }

    private enum ProbeOutcome {
        HEALTHY,
        UNHEALTHY,
        TIMEOUT
    }

    private record PendingActivation(
        String matchId,
        CompletableFuture<AllocationSnapshot> result
    ) {
    }

    private record Tombstone(RoomSnapshot snapshot, Instant expiresAt) {
    }
}
