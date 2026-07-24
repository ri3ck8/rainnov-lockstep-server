package com.rainnov.lockstep.room;

import com.rainnov.lockstep.protocol.Envelope;

public interface DataPlaneSession {

    String sessionId();

    boolean isWritable();

    void send(Envelope envelope);

    void close(int statusCode, String reason);
}
