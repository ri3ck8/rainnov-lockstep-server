package com.rainnov.lockstep.transport;

final class DataPlaneCloseCodes {

    static final int AUTHENTICATION_FAILED = 4001;
    static final int HELLO_TIMEOUT = 4002;
    static final int ROOM_REJECTED = 4003;

    private DataPlaneCloseCodes() {
    }
}
