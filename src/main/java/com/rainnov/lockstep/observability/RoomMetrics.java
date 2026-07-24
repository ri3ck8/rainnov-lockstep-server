package com.rainnov.lockstep.observability;

import com.rainnov.lockstep.room.CapacitySnapshot;
import com.rainnov.lockstep.room.RoomPoolManager;
import com.rainnov.lockstep.room.RoomSnapshot;
import com.rainnov.lockstep.room.TerminationReason;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public final class RoomMetrics {

    private final RoomPoolManager roomPool;
    private final MeterRegistry registry;
    private final AtomicInteger initializingRooms = new AtomicInteger();
    private final AtomicInteger readyRooms = new AtomicInteger();
    private final AtomicInteger activatingRooms = new AtomicInteger();
    private final AtomicInteger activeRooms = new AtomicInteger();
    private final AtomicInteger failedRooms = new AtomicInteger();
    private final AtomicInteger terminatingRooms = new AtomicInteger();
    private final AtomicInteger healthyRooms = new AtomicInteger();
    private final AtomicInteger totalRooms = new AtomicInteger();
    private final AtomicInteger pendingReplacements = new AtomicInteger();
    private final AtomicLong maximumTickLagNanos = new AtomicLong();
    private final ConcurrentLinkedQueue<ReplacementObservation> replacements =
        new ConcurrentLinkedQueue<>();
    private final Counter allocationSuccess;
    private final Counter allocationCapacityExhausted;
    private final Counter abnormalTerminations;
    private final Counter replacementFailures;
    private final Timer activationLatency;
    private final Timer replacementLag;
    private final ScheduledExecutorService sampler =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("room-metrics-", 0).factory()
        );

    public RoomMetrics(
        RoomPoolManager roomPool,
        MeterRegistry registry
    ) {
        this.roomPool = roomPool;
        this.registry = registry;

        roomGauge(registry, "initializing", initializingRooms);
        roomGauge(registry, "ready", readyRooms);
        roomGauge(registry, "activating", activatingRooms);
        roomGauge(registry, "active", activeRooms);
        roomGauge(registry, "failed", failedRooms);
        roomGauge(registry, "terminating", terminatingRooms);
        roomGauge(registry, "healthy", healthyRooms);
        roomGauge(registry, "total", totalRooms);
        Gauge.builder(
            "lockstep.room.replacements.pending",
            pendingReplacements,
            AtomicInteger::get
        ).register(registry);
        Gauge.builder(
            "lockstep.tick.lag.seconds",
            maximumTickLagNanos,
            value -> value.get() / 1_000_000_000.0
        ).description("Maximum last observed tick lag across live rooms")
            .register(registry);

        allocationSuccess = Counter.builder("lockstep.allocations")
            .tag("result", "success")
            .register(registry);
        allocationCapacityExhausted = Counter.builder("lockstep.allocations")
            .tag("result", "capacity_exhausted")
            .register(registry);
        abnormalTerminations = Counter.builder("lockstep.room.terminations")
            .tag("type", "abnormal")
            .register(registry);
        replacementFailures = Counter.builder("lockstep.room.replacements.failures")
            .description("Room creation attempts that failed during pool reconciliation")
            .register(registry);
        activationLatency = Timer.builder("lockstep.room.activation")
            .description("Control-plane room activation latency")
            .publishPercentileHistogram()
            .register(registry);
        replacementLag = Timer.builder("lockstep.room.replacement.lag")
            .description("Time from one-shot room termination until pool restoration")
            .publishPercentileHistogram()
            .register(registry);
    }

    @PostConstruct
    void start() {
        roomPool.addTerminationListener(this::recordTermination);
        roomPool.addRoomCreationFailureListener(replacementFailures::increment);
        sampler.scheduleWithFixedDelay(this::sample, 0, 1, TimeUnit.SECONDS);
    }

    public void recordAllocationSuccess(Duration activationTime) {
        allocationSuccess.increment();
        activationLatency.record(activationTime);
    }

    public void recordCapacityExhausted() {
        allocationCapacityExhausted.increment();
    }

    private void sample() {
        roomPool.capacity()
            .thenCombine(roomPool.liveSnapshots(), MetricsSample::new)
            .whenComplete((sample, error) -> {
                if (error != null) {
                    return;
                }
                updateRoomGauges(sample.capacity());
                maximumTickLagNanos.set(sample.rooms().stream()
                    .mapToLong(RoomSnapshot::lastTickLagNanos)
                    .max()
                    .orElse(0));
                updateReplacements(sample.capacity());
            });
    }

    private void updateRoomGauges(CapacitySnapshot capacity) {
        initializingRooms.set(capacity.initializingRooms());
        readyRooms.set(capacity.readyRooms());
        activatingRooms.set(capacity.activatingRooms());
        activeRooms.set(capacity.activeRooms());
        failedRooms.set(capacity.failedRooms());
        terminatingRooms.set(capacity.terminatingRooms());
        healthyRooms.set(capacity.healthyRooms());
        totalRooms.set(capacity.totalLiveRooms());
    }

    private void updateReplacements(CapacitySnapshot capacity) {
        if (!capacity.acceptingAllocations()) {
            replacements.clear();
            pendingReplacements.set(0);
            return;
        }
        long now = System.nanoTime();
        if (capacity.totalLiveRooms() == capacity.targetRooms()
            && capacity.initializingRooms() == 0) {
            ReplacementObservation replacement;
            while ((replacement = replacements.poll()) != null) {
                replacementLag.record(
                    Math.max(0, now - replacement.startedNanos),
                    TimeUnit.NANOSECONDS
                );
            }
        }
        pendingReplacements.set(replacements.size());
    }

    private void recordTermination(RoomSnapshot room) {
        TerminationReason reason = room.terminationReason();
        if (reason == null) {
            return;
        }
        registry.counter(
            "lockstep.room.terminations.by.reason",
            "reason",
            reason.name().toLowerCase(java.util.Locale.ROOT)
        ).increment();
        if (reason != TerminationReason.MATCH_COMPLETED
            && reason != TerminationReason.ADMINISTRATIVE
            && reason != TerminationReason.NODE_DRAINING) {
            abnormalTerminations.increment();
        }
        if (reason != TerminationReason.NODE_DRAINING) {
            replacements.add(new ReplacementObservation(System.nanoTime()));
            pendingReplacements.incrementAndGet();
        }
    }

    @PreDestroy
    void stop() {
        sampler.shutdownNow();
    }

    private static void roomGauge(
        MeterRegistry registry,
        String state,
        AtomicInteger value
    ) {
        Gauge.builder("lockstep.rooms", value, AtomicInteger::get)
            .tag("state", state)
            .register(registry);
    }

    private record MetricsSample(
        CapacitySnapshot capacity,
        List<RoomSnapshot> rooms
    ) {
    }

    private static final class ReplacementObservation {

        private final long startedNanos;

        private ReplacementObservation(long startedNanos) {
            this.startedNanos = startedNanos;
        }
    }
}
