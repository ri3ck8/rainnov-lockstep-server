package com.rainnov.lockstep.transport;

import com.rainnov.lockstep.protocol.Envelope;
import com.rainnov.lockstep.room.DataPlaneSession;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.util.concurrent.ScheduledFuture;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A thread-safe room session backed by one Netty WebSocket channel.
 */
public final class NettyDataPlaneSession implements DataPlaneSession {

    private static final int MAX_CLOSE_REASON_BYTES = 123;
    private static final Duration DEFAULT_CLOSE_FLUSH_TIMEOUT = Duration.ofSeconds(5);

    private final Object submissionMonitor = new Object();
    private final String sessionId;
    private final Channel channel;
    private final long closeFlushTimeoutNanos;
    private final DataPlaneTelemetry telemetry;
    private final AtomicBoolean closing = new AtomicBoolean();

    public NettyDataPlaneSession(Channel channel) {
        this(
            UUID.randomUUID().toString(),
            channel,
            DEFAULT_CLOSE_FLUSH_TIMEOUT,
            DataPlaneTelemetry.NOOP
        );
    }

    NettyDataPlaneSession(Channel channel, Duration closeFlushTimeout) {
        this(
            UUID.randomUUID().toString(),
            channel,
            closeFlushTimeout,
            DataPlaneTelemetry.NOOP
        );
    }

    NettyDataPlaneSession(
        Channel channel,
        Duration closeFlushTimeout,
        DataPlaneTelemetry telemetry
    ) {
        this(UUID.randomUUID().toString(), channel, closeFlushTimeout, telemetry);
    }

    NettyDataPlaneSession(String sessionId, Channel channel) {
        this(
            sessionId,
            channel,
            DEFAULT_CLOSE_FLUSH_TIMEOUT,
            DataPlaneTelemetry.NOOP
        );
    }

    NettyDataPlaneSession(
        String sessionId,
        Channel channel,
        Duration closeFlushTimeout,
        DataPlaneTelemetry telemetry
    ) {
        this.sessionId = requireText(sessionId, "sessionId");
        this.channel = Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(closeFlushTimeout, "closeFlushTimeout");
        if (closeFlushTimeout.isNegative() || closeFlushTimeout.isZero()) {
            throw new IllegalArgumentException("closeFlushTimeout must be positive");
        }
        this.closeFlushTimeoutNanos = closeFlushTimeout.toNanos();
        this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
    }

    @Override
    public String sessionId() {
        return sessionId;
    }

    @Override
    public boolean isWritable() {
        return channel.isActive() && channel.isWritable() && !closing.get();
    }

    @Override
    public void send(Envelope envelope) {
        Objects.requireNonNull(envelope, "envelope");
        synchronized (submissionMonitor) {
            // The monitor makes acceptance and EventLoop submission atomic
            // relative to close(), so accepted writes always precede CloseFrame.
            if (closing.get()) {
                return;
            }
            runOnEventLoop(() -> writeEnvelope(envelope));
        }
    }

    @Override
    public void close(int statusCode, String reason) {
        if (closeAfter(null, statusCode, reason)
            && statusCode
            == com.rainnov.lockstep.room.SessionCloseCodes.HEARTBEAT_TIMEOUT) {
            telemetry.recordHeartbeatTimeout();
        }
    }

    boolean closeAfter(Envelope finalEnvelope, int statusCode, String reason) {
        synchronized (submissionMonitor) {
            if (!closing.compareAndSet(false, true)) {
                return false;
            }
            String safeReason = truncateCloseReason(reason);
            enqueueOnEventLoop(() ->
                writeFinalEnvelopeAndClose(finalEnvelope, statusCode, safeReason)
            );
            return true;
        }
    }

    private void writeEnvelope(Envelope envelope) {
        if (!channel.isActive()) {
            return;
        }
        if (!channel.isWritable()) {
            close(
                com.rainnov.lockstep.room.SessionCloseCodes.SLOW_CONSUMER,
                "SLOW_CONSUMER"
            );
            return;
        }
        BinaryWebSocketFrame frame = new BinaryWebSocketFrame(
            Unpooled.wrappedBuffer(envelope.toByteArray())
        );
        channel.writeAndFlush(frame).addListener(future -> {
            if (!future.isSuccess()) {
                channel.close();
            }
        });
    }

    private void writeFinalEnvelopeAndClose(
        Envelope finalEnvelope,
        int statusCode,
        String safeReason
    ) {
        if (!channel.isActive()) {
            channel.close();
            return;
        }
        ScheduledFuture<?> hardClose = channel.eventLoop().schedule(
            (Runnable) channel::close,
            closeFlushTimeoutNanos,
            TimeUnit.NANOSECONDS
        );
        try {
            if (finalEnvelope != null) {
                channel.write(new BinaryWebSocketFrame(
                    Unpooled.wrappedBuffer(finalEnvelope.toByteArray())
                ));
            }
            channel.writeAndFlush(new CloseWebSocketFrame(statusCode, safeReason))
                .addListener(ignored -> {
                    hardClose.cancel(false);
                    channel.close();
                });
        } catch (RuntimeException error) {
            hardClose.cancel(false);
            channel.close();
        }
    }

    private void runOnEventLoop(Runnable command) {
        if (channel.eventLoop().inEventLoop()) {
            command.run();
            return;
        }
        try {
            channel.eventLoop().execute(command);
        } catch (RuntimeException ignored) {
            channel.close();
        }
    }

    private void enqueueOnEventLoop(Runnable command) {
        try {
            channel.eventLoop().execute(command);
        } catch (RuntimeException ignored) {
            channel.close();
        }
    }

    static String truncateCloseReason(String reason) {
        if (reason == null || reason.isEmpty()) {
            return "";
        }
        if (reason.getBytes(StandardCharsets.UTF_8).length <= MAX_CLOSE_REASON_BYTES) {
            return reason;
        }
        int end = reason.length();
        while (end > 0) {
            end = reason.offsetByCodePoints(end, -1);
            String candidate = reason.substring(0, end);
            if (candidate.getBytes(StandardCharsets.UTF_8).length <= MAX_CLOSE_REASON_BYTES) {
                return candidate;
            }
        }
        return "";
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
