package com.rainnov.lockstep.application;

import com.rainnov.lockstep.room.AllocationSnapshot;

import java.util.List;

public record AllocationOutcome(
    AllocationSnapshot allocation,
    boolean created,
    List<PlayerTicket> playerTickets
) {
    public AllocationOutcome {
        playerTickets = List.copyOf(playerTickets);
    }
}
