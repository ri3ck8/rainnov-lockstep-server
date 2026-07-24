package com.rainnov.lockstep.room;

public final class SessionCloseCodes {

    public static final int SESSION_REPLACED = 4006;
    public static final int HEARTBEAT_TIMEOUT = 4007;
    public static final int ROOM_TERMINATED = 4008;
    public static final int SLOW_CONSUMER = 4009;

    private SessionCloseCodes() {
    }
}
