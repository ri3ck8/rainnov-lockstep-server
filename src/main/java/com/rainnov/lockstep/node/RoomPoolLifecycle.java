package com.rainnov.lockstep.node;

import com.rainnov.lockstep.config.LockstepProperties;
import com.rainnov.lockstep.room.RoomPoolManager;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public final class RoomPoolLifecycle implements SmartLifecycle {

    public static final int PHASE = 100;

    private final RoomPoolManager roomPool;
    private final NodeLifecycleManager nodeLifecycle;
    private final LockstepProperties properties;
    private final AtomicBoolean running = new AtomicBoolean();

    public RoomPoolLifecycle(
        RoomPoolManager roomPool,
        NodeLifecycleManager nodeLifecycle,
        LockstepProperties properties
    ) {
        this.roomPool = roomPool;
        this.nodeLifecycle = nodeLifecycle;
        this.properties = properties;
        roomPool.addFatalNodeFailureListener(ignored -> {
            nodeLifecycle.beginDraining();
            roomPool.drain(Duration.ZERO).whenComplete((unused, error) ->
                nodeLifecycle.markTerminated()
            );
        });
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            roomPool.start().toCompletableFuture().get(30, TimeUnit.SECONDS);
            nodeLifecycle.markReady();
        } catch (Exception exception) {
            running.set(false);
            throw new ApplicationContextException("Failed to initialize the room pool", exception);
        }
    }

    @Override
    public void stop(Runnable callback) {
        if (!running.compareAndSet(true, false)) {
            callback.run();
            return;
        }
        nodeLifecycle.beginDraining();
        roomPool.drain(properties.getNode().getShutdownGrace())
            .whenComplete((ignored, error) -> {
                nodeLifecycle.markTerminated();
                callback.run();
            });
    }

    @Override
    public void stop() {
        stop(() -> {
        });
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }
}
