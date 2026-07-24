package com.rainnov.lockstep.room;

public class RoomException extends RuntimeException {

    private final String code;

    public RoomException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
