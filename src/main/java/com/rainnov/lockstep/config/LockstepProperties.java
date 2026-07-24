package com.rainnov.lockstep.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "lockstep")
public class LockstepProperties {

    @Valid
    private final Node node = new Node();

    @Valid
    private final Pool pool = new Pool();

    @Valid
    private final Room room = new Room();

    @Valid
    private final Frame frame = new Frame();

    @Valid
    private final DataPlane dataPlane = new DataPlane();

    @Valid
    private final Security security = new Security();

    @Getter
    @Setter
    public static class Node {
        @NotBlank
        @Size(max = 256)
        @Pattern(
            regexp = "^(?!.*\\p{Cc})\\S(?:.*\\S)?$",
            message = "must not have surrounding whitespace or control characters"
        )
        private String id;

        @NotNull
        private Duration shutdownGrace = Duration.ofSeconds(30);

        @AssertTrue(message = "shutdown-grace must not be negative")
        public boolean isShutdownGraceValid() {
            return shutdownGrace != null && !shutdownGrace.isNegative();
        }
    }

    @Getter
    @Setter
    public static class Pool {
        @Min(1)
        private int targetSize = 16;

        @Min(1)
        private int roomExecutorThreads = 4;

        @NotNull
        private Duration tombstoneRetention = Duration.ofMinutes(10);

        @NotNull
        private Duration healthCheckInterval = Duration.ofSeconds(10);

        @Min(1)
        private int healthFailureThreshold = 3;
    }

    @Getter
    @Setter
    public static class Room {
        @Min(1)
        @Max(128)
        private int maxPlayers = 8;

        @NotNull
        private Duration joinTimeout = Duration.ofSeconds(60);

        @NotNull
        private Duration reconnectGrace = Duration.ofSeconds(30);

        @NotNull
        private Duration maxDuration = Duration.ofHours(1);

        @NotNull
        private Duration gracefulTerminationTimeout = Duration.ofSeconds(5);
    }

    @Getter
    @Setter
    public static class Frame {
        @Min(1)
        @Max(120)
        private int tickRate = 20;

        @Min(1)
        private int inputDelayFrames = 2;

        @Min(1)
        private int maxLeadFrames = 4;

        @Min(1)
        private int historySeconds = 50;

        @Min(1)
        @Max(65536)
        private int maxInputBytes = 1024;

        public int historyFrames() {
            return Math.multiplyExact(tickRate, historySeconds);
        }
    }

    @Getter
    @Setter
    public static class DataPlane {
        // 端口 0 适用于隔离的集成测试；生产部署应配置稳定的对外服务端点。
        @Min(0)
        @Max(65535)
        private int port = 9000;

        @NotBlank
        private String path = "/game";

        @NotBlank
        private String subprotocol = "lockstep.protobuf.v1";

        @NotNull
        private URI advertisedUri = URI.create("ws://localhost:9000/game");

        @Min(1)
        private int protocolVersion = 1;

        @NotNull
        private Duration authenticationTimeout = Duration.ofSeconds(5);

        @NotNull
        private Duration clientPingInterval = Duration.ofSeconds(5);

        @NotNull
        private Duration connectionIdleTimeout = Duration.ofSeconds(15);

        @Min(1024)
        private int maxWebsocketFrameBytes = 65536;

        @Min(1024)
        private int writeBufferLowWaterMark = 32768;

        @Min(2048)
        private int writeBufferHighWaterMark = 65536;
    }

    @Getter
    @Setter
    public static class Security {
        @NotBlank
        @Size(min = 8, max = 1024)
        private String apiKey;

        @NotBlank
        @Size(min = 16, max = 4096)
        private String ticketSecret;
    }
}
