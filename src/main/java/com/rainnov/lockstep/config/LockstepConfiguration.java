package com.rainnov.lockstep.config;

import com.rainnov.lockstep.security.ticket.HmacTicketService;
import com.rainnov.lockstep.security.ticket.TicketService;
import com.rainnov.lockstep.room.NettyRoomEventLoopProvider;
import com.rainnov.lockstep.room.RoomPoolManager;
import com.rainnov.lockstep.room.RoomEventLoopProvider;
import com.rainnov.lockstep.room.RoomSettings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;
import java.time.Clock;

@Configuration(proxyBeanMethods = false)
public class LockstepConfiguration {

    @Bean
    Clock lockstepClock() {
        return Clock.systemUTC();
    }

    @Bean
    TicketService ticketService(LockstepProperties properties, Clock lockstepClock) {
        byte[] secret = properties.getSecurity().getTicketSecret().getBytes(StandardCharsets.UTF_8);
        return new HmacTicketService(secret, lockstepClock);
    }

    @Bean(destroyMethod = "close")
    RoomEventLoopProvider roomEventLoopProvider(LockstepProperties properties) {
        return new NettyRoomEventLoopProvider(properties.getPool().getRoomExecutorThreads());
    }

    @Bean
    RoomSettings roomSettings(LockstepProperties properties) {
        return new RoomSettings(
            properties.getDataPlane().getProtocolVersion(),
            properties.getRoom().getMaxPlayers(),
            properties.getFrame().getTickRate(),
            properties.getFrame().getInputDelayFrames(),
            properties.getFrame().getMaxLeadFrames(),
            properties.getRoom().getJoinTimeout(),
            properties.getDataPlane().getClientPingInterval(),
            properties.getDataPlane().getConnectionIdleTimeout(),
            properties.getRoom().getReconnectGrace(),
            properties.getRoom().getMaxDuration(),
            properties.getFrame().historyFrames(),
            properties.getFrame().getMaxInputBytes()
        );
    }

    @Bean
    RoomPoolManager roomPoolManager(
        LockstepProperties properties,
        RoomSettings roomSettings,
        RoomEventLoopProvider roomEventLoopProvider
    ) {
        return new RoomPoolManager(
            properties.getNode().getId(),
            properties.getPool().getTargetSize(),
            roomSettings,
            roomEventLoopProvider,
            properties.getPool().getTombstoneRetention(),
            properties.getPool().getHealthCheckInterval(),
            properties.getPool().getHealthFailureThreshold()
        );
    }
}
