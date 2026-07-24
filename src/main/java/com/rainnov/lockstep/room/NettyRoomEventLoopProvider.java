package com.rainnov.lockstep.room;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class NettyRoomEventLoopProvider implements RoomEventLoopProvider {

    private final DefaultEventExecutorGroup rooms;
    private final AtomicBoolean closed = new AtomicBoolean();

    public NettyRoomEventLoopProvider(int roomThreads) {
        if (roomThreads <= 0) {
            throw new IllegalArgumentException("roomThreads must be positive");
        }
        this.rooms = new DefaultEventExecutorGroup(
            roomThreads,
            new DefaultThreadFactory("lockstep-room", true)
        );
    }

    @Override
    public EventExecutor next() {
        return rooms.next();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        rooms.shutdownGracefully(0, 5, TimeUnit.SECONDS);
    }
}
