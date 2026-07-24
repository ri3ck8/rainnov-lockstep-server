package com.rainnov.lockstep.room;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RoomPoolManagerTest {

    private RoomPoolManager manager;

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.close();
        }
    }

    @Test
    void initializesFixedCapacityAndAllocatesEachRoomAtMostOnce() throws Exception {
        manager = manager(4);
        CapacitySnapshot started = await(manager.start());
        assertThat(started.readyRooms()).isEqualTo(4);
        assertThat(started.totalLiveRooms()).isEqualTo(4);

        List<CompletableFuture<AllocationSnapshot>> attempts = new ArrayList<>();
        for (int index = 0; index < 12; index++) {
            attempts.add(
                manager.allocate("match-" + index, List.of("p-" + index))
                    .toCompletableFuture()
            );
        }

        CompletableFuture.allOf(
            attempts.stream()
                .map(future -> future.exceptionally(ignored -> null))
                .toArray(CompletableFuture[]::new)
        ).get(3, TimeUnit.SECONDS);

        List<AllocationSnapshot> successes = attempts.stream()
            .filter(future -> !future.isCompletedExceptionally())
            .map(CompletableFuture::join)
            .toList();
        assertThat(successes).hasSize(4);
        assertThat(successes).extracting(AllocationSnapshot::roomId)
            .doesNotHaveDuplicates();
        assertThat(await(manager.capacity()).activeRooms()).isEqualTo(4);
    }

    @Test
    void terminalRoomIsReplacedWithFreshIdentityExactlyOnce() throws Exception {
        manager = manager(2);
        await(manager.start());
        List<RoomSnapshot> terminal = new java.util.concurrent.CopyOnWriteArrayList<>();
        manager.addTerminationListener(terminal::add);
        AllocationSnapshot allocation =
            await(manager.allocate("match-1", List.of("p1")));

        CompletableFuture<RoomSnapshot> first = manager.terminate(
            allocation.roomId(),
            allocation.matchId(),
            TerminationMode.GRACEFUL,
            TerminationReason.MATCH_COMPLETED
        ).toCompletableFuture();
        CompletableFuture<RoomSnapshot> second = manager.terminate(
            allocation.roomId(),
            allocation.matchId(),
            TerminationMode.GRACEFUL,
            TerminationReason.MATCH_COMPLETED
        ).toCompletableFuture();
        assertThat(first.get(3, TimeUnit.SECONDS).state()).isEqualTo(RoomState.TERMINATED);
        assertThat(second.get(3, TimeUnit.SECONDS).state()).isEqualTo(RoomState.TERMINATED);

        waitUntil(
            () -> {
                CapacitySnapshot capacity = await(manager.capacity());
                return capacity.totalLiveRooms() == 2 && capacity.readyRooms() == 2;
            },
            Duration.ofSeconds(2)
        );
        assertThat(terminal).hasSize(1);
        assertThat(await(manager.roomSnapshot(allocation.roomId())).state())
            .isEqualTo(RoomState.TERMINATED);
        assertThatThrownBy(() -> manager.terminate(
            allocation.roomId(),
            "different-match",
            TerminationMode.FORCE,
            TerminationReason.ADMINISTRATIVE
        ).toCompletableFuture().join())
            .hasRootCauseInstanceOf(RoomException.class)
            .rootCause()
            .extracting(error -> ((RoomException) error).code())
            .isEqualTo("MATCH_MISMATCH");

        AllocationSnapshot replacement =
            await(manager.allocate("match-2", List.of("p2")));
        assertThat(replacement.roomId()).isNotEqualTo(allocation.roomId());
    }

    @Test
    void sameMatchCannotBeAllocatedConcurrently() throws Exception {
        manager = manager(3);
        await(manager.start());

        List<CompletableFuture<AllocationSnapshot>> attempts = List.of(
            manager.allocate("same-match", List.of("p1")).toCompletableFuture(),
            manager.allocate("same-match", List.of("p1")).toCompletableFuture(),
            manager.allocate("same-match", List.of("p1")).toCompletableFuture()
        );
        CompletableFuture.allOf(
            attempts.stream()
                .map(future -> future.exceptionally(ignored -> null))
                .toArray(CompletableFuture[]::new)
        ).get(3, TimeUnit.SECONDS);
        assertThat(attempts.stream().filter(future -> !future.isCompletedExceptionally()))
            .hasSize(1);
    }

    @Test
    void drainRejectsAllocationsAndForceTerminatesActiveRooms() throws Exception {
        manager = manager(2);
        await(manager.start());
        await(manager.allocate("match-1", List.of("p1")));

        await(manager.drain(Duration.ofMillis(20)));
        CapacitySnapshot drained = await(manager.capacity());
        assertThat(drained.acceptingAllocations()).isFalse();
        assertThat(drained.totalLiveRooms()).isZero();
        assertThatThrownBy(() ->
            manager.allocate("match-2", List.of("p2")).toCompletableFuture().join()
        ).hasRootCauseInstanceOf(RoomException.class);
    }

    @Test
    void healthDeadlineDrainsNodeWhoseRoomEventLoopNeverCompletesProbe() throws Exception {
        BlockingRoomEventLoopProvider provider = new BlockingRoomEventLoopProvider();
        manager = manager(1, provider, Duration.ofMillis(20), 2);
        await(manager.start());
        AllocationSnapshot allocation =
            await(manager.allocate("match-stuck", List.of("p1")));
        List<RoomSnapshot> terminal = new java.util.concurrent.CopyOnWriteArrayList<>();
        List<TerminationReason> fatalFailures =
            new java.util.concurrent.CopyOnWriteArrayList<>();
        manager.addTerminationListener(terminal::add);
        manager.addFatalNodeFailureListener(fatalFailures::add);

        provider.block();
        waitUntil(
            () -> terminal.stream().anyMatch(snapshot ->
                snapshot.roomId().equals(allocation.roomId())
                    && snapshot.terminationReason()
                    == TerminationReason.HEALTH_CHECK_FAILED
            ),
            Duration.ofSeconds(2)
        );

        RoomSnapshot tombstone = await(manager.roomSnapshot(allocation.roomId()));
        assertThat(tombstone.state()).isEqualTo(RoomState.TERMINATED);
        assertThat(tombstone.terminationMode()).isEqualTo(TerminationMode.FORCE);
        CapacitySnapshot failedNode = await(manager.capacity());
        assertThat(failedNode.acceptingAllocations()).isFalse();
        assertThat(failedNode.totalLiveRooms()).isZero();
        assertThat(fatalFailures)
            .containsExactly(TerminationReason.HEALTH_CHECK_FAILED);

        provider.unblock();
        Thread.sleep(100);
        assertThat(await(manager.capacity()).totalLiveRooms()).isZero();
        assertThat(terminal.stream()
            .filter(snapshot -> snapshot.roomId().equals(allocation.roomId())))
            .hasSize(1);
    }

    @Test
    void drainCompletesWithinBoundWhenRoomEventLoopNeverRunsTermination() throws Exception {
        BlockingRoomEventLoopProvider provider = new BlockingRoomEventLoopProvider();
        manager = manager(1, provider, Duration.ofMillis(25), 3);
        await(manager.start());
        AllocationSnapshot allocation =
            await(manager.allocate("match-stuck", List.of("p1")));
        List<RoomSnapshot> terminal = new java.util.concurrent.CopyOnWriteArrayList<>();
        manager.addTerminationListener(terminal::add);
        provider.block();

        long startedAt = System.nanoTime();
        await(manager.drain(Duration.ofMillis(25)));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        assertThat(elapsed).isLessThan(Duration.ofMillis(500));
        assertThat(await(manager.capacity()).totalLiveRooms()).isZero();
        RoomSnapshot tombstone = await(manager.roomSnapshot(allocation.roomId()));
        assertThat(tombstone.terminationReason())
            .isEqualTo(TerminationReason.NODE_DRAINING);
        assertThat(tombstone.terminationMode()).isEqualTo(TerminationMode.FORCE);
        assertThat(terminal).hasSize(1);

        provider.unblock();
    }

    @Test
    void drainFailsPendingActivationInsteadOfReturningTerminatedRoom() throws Exception {
        BlockingRoomEventLoopProvider provider = new BlockingRoomEventLoopProvider();
        manager = manager(1, provider, Duration.ofMillis(25), 3);
        await(manager.start());
        provider.block();

        CompletableFuture<AllocationSnapshot> allocation =
            manager.allocate("match-pending", List.of("p1")).toCompletableFuture();
        waitUntil(
            () -> await(manager.capacity()).activatingRooms() == 1,
            Duration.ofSeconds(1)
        );

        await(manager.drain(Duration.ofMillis(25)));
        assertThatThrownBy(() -> allocation.get(1, TimeUnit.SECONDS))
            .hasRootCauseInstanceOf(RoomException.class)
            .rootCause()
            .extracting(error -> ((RoomException) error).code())
            .isEqualTo("NODE_DRAINING");
        assertThat(await(manager.capacity()).totalLiveRooms()).isZero();

        provider.unblock();
    }

    @Test
    @SuppressWarnings("unchecked")
    void healthSweepRepairsReadyRoomIndexBeforeAllocating() throws Exception {
        manager = manager(
            2,
            new NettyRoomEventLoopProvider(2),
            Duration.ofMillis(20),
            3
        );
        await(manager.start());
        List<String> roomIds = await(manager.liveSnapshots()).stream()
            .map(RoomSnapshot::roomId)
            .toList();

        java.lang.reflect.Field readyIndex =
            RoomPoolManager.class.getDeclaredField("readyRoomIds");
        readyIndex.setAccessible(true);
        java.lang.reflect.Field coordinatorField =
            RoomPoolManager.class.getDeclaredField("coordinator");
        coordinatorField.setAccessible(true);
        EventExecutor coordinator =
            (EventExecutor) coordinatorField.get(manager);
        ArrayDeque<String> ready = (ArrayDeque<String>) readyIndex.get(manager);
        coordinator.submit(() -> {
            ready.clear();
            ready.add(roomIds.getFirst());
            ready.add(roomIds.getFirst());
            ready.add("missing-room");
        }).get(1, TimeUnit.SECONDS);

        waitUntil(
            () -> coordinator.submit(() ->
                ready.size() == 2 && ready.containsAll(roomIds)
            ).get(1, TimeUnit.SECONDS),
            Duration.ofSeconds(1)
        );

        AllocationSnapshot first =
            await(manager.allocate("match-index-1", List.of("p1")));
        AllocationSnapshot second =
            await(manager.allocate("match-index-2", List.of("p2")));
        assertThat(List.of(first.roomId(), second.roomId()))
            .containsExactlyInAnyOrderElementsOf(roomIds);
    }

    private static RoomPoolManager manager(int capacity) {
        return manager(
            capacity,
            new NettyRoomEventLoopProvider(Math.max(2, capacity)),
            Duration.ofSeconds(5),
            3
        );
    }

    private static RoomPoolManager manager(
        int capacity,
        RoomEventLoopProvider loops,
        Duration healthInterval,
        int healthFailureThreshold
    ) {
        AtomicInteger roomIds = new AtomicInteger();
        AtomicInteger allocationIds = new AtomicInteger();
        return new RoomPoolManager(
            "node-1",
            capacity,
            new RoomSettings(
                1,
                8,
                20,
                2,
                4,
                Duration.ofSeconds(5),
                Duration.ofSeconds(1),
                Duration.ofSeconds(5),
                Duration.ofSeconds(5),
                Duration.ofSeconds(10),
                100,
                1024
            ),
            loops,
            Duration.ofMinutes(10),
            healthInterval,
            healthFailureThreshold,
            Clock.systemUTC(),
            () -> "room-" + roomIds.incrementAndGet(),
            () -> "allocation-" + allocationIds.incrementAndGet()
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

    private static final class BlockingRoomEventLoopProvider
        implements RoomEventLoopProvider {

        private final DefaultEventExecutor executor = new DefaultEventExecutor(
            new DefaultThreadFactory("test-blocked-room", true)
        );
        private final CountDownLatch blockerStarted = new CountDownLatch(1);
        private final CountDownLatch unblock = new CountDownLatch(1);

        @Override
        public EventExecutor next() {
            return executor;
        }

        private void block() throws Exception {
            executor.execute(() -> {
                blockerStarted.countDown();
                try {
                    unblock.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(blockerStarted.await(1, TimeUnit.SECONDS)).isTrue();
        }

        private void unblock() {
            unblock.countDown();
        }

        @Override
        public void close() {
            unblock();
            executor.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
    }
}
