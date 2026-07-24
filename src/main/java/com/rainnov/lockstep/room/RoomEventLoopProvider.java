package com.rainnov.lockstep.room;

import io.netty.util.concurrent.EventExecutor;

public interface RoomEventLoopProvider extends AutoCloseable {

    EventExecutor next();

    @Override
    void close();
}
