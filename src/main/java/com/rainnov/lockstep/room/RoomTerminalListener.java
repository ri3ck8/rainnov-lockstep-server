package com.rainnov.lockstep.room;

@FunctionalInterface
public interface RoomTerminalListener {

    void onTerminated(GameRoom room, RoomSnapshot terminalSnapshot);
}
