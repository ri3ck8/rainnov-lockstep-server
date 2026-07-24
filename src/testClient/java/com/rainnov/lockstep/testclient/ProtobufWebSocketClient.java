package com.rainnov.lockstep.testclient;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rainnov.lockstep.protocol.ClientHello;
import com.rainnov.lockstep.protocol.ClientInput;
import com.rainnov.lockstep.protocol.ClientPing;
import com.rainnov.lockstep.protocol.Envelope;
import com.rainnov.lockstep.protocol.EventType;
import com.rainnov.lockstep.protocol.ServerFrame;
import com.rainnov.lockstep.protocol.ServerHello;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

final class ProtobufWebSocketClient implements WebSocket.Listener, AutoCloseable {

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .build();

    private final String playerId;
    private final Duration timeout;
    private final BlockingQueue<Object> incoming = new LinkedBlockingQueue<>();
    private final ByteArrayOutputStream binaryMessage = new ByteArrayOutputStream();
    private final Object sendLock = new Object();
    private final ScheduledExecutorService heartbeatExecutor =
        Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
            .daemon()
            .name("frame-sync-heartbeat-", 0)
            .factory());
    private final AtomicLong pingSequence = new AtomicLong();
    private volatile WebSocket socket;
    private CompletableFuture<Void> sendTail = CompletableFuture.completedFuture(null);
    private ScheduledFuture<?> heartbeat;

    private ProtobufWebSocketClient(String playerId, Duration timeout) {
        this.playerId = playerId;
        this.timeout = timeout;
    }

    static ProtobufWebSocketClient connect(
        String playerId,
        URI uri,
        String subprotocol,
        Duration timeout
    ) throws Exception {
        ProtobufWebSocketClient listener =
            new ProtobufWebSocketClient(playerId, timeout);
        WebSocket socket = HTTP_CLIENT.newWebSocketBuilder()
            .connectTimeout(timeout)
            .subprotocols(subprotocol)
            .buildAsync(uri, listener)
            .get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        listener.socket = socket;
        if (!subprotocol.equals(socket.getSubprotocol())) {
            listener.close();
            throw new IllegalStateException(
                "玩家 " + playerId + " 的 WebSocket 子协议不匹配："
                    + socket.getSubprotocol()
            );
        }
        return listener;
    }

    ServerHello authenticate(
        ControlPlaneClient.AllocationResponse allocation,
        ControlPlaneClient.PlayerTicket ticket
    ) throws Exception {
        Envelope hello = Envelope.newBuilder()
            .setProtocolVersion(allocation.protocolVersion())
            .setRequestId("hello-" + playerId)
            .setClientHello(ClientHello.newBuilder()
                .setRoomId(allocation.roomId())
                .setMatchId(allocation.matchId())
                .setPlayerId(playerId)
                .setTicket(ticket.ticket())
                .setLastAppliedFrame(0))
            .build();
        sendAndWait(hello);
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            TimedEnvelope message = take(deadline);
            rejectProtocolError(message.envelope());
            if (message.envelope().hasServerHello()) {
                return message.envelope().getServerHello();
            }
        }
    }

    void awaitMatchStarted() throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            TimedEnvelope message = take(deadline);
            rejectProtocolError(message.envelope());
            if (message.envelope().hasMatchEvent()
                && message.envelope().getMatchEvent().getType()
                == EventType.EVENT_TYPE_MATCH_STARTED) {
                return;
            }
        }
    }

    ReceivedFrame awaitFrame(long expectedFrame) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            TimedEnvelope message = take(deadline);
            Envelope envelope = message.envelope();
            rejectProtocolError(envelope);
            if (envelope.hasMatchEvent()
                && envelope.getMatchEvent().getType()
                == EventType.EVENT_TYPE_MATCH_ENDED) {
                throw new IllegalStateException(
                    "玩家 " + playerId + " 在等待第 " + expectedFrame
                        + " 帧时收到对局结束事件"
                );
            }
            if (!envelope.hasServerFrame()) {
                continue;
            }
            ServerFrame frame = envelope.getServerFrame();
            long actualFrame = Integer.toUnsignedLong(frame.getFrameId());
            if (actualFrame != expectedFrame) {
                throw new IllegalStateException(
                    "玩家 " + playerId + " 期望第 " + expectedFrame
                        + " 帧，实际收到第 " + actualFrame + " 帧"
                );
            }
            return new ReceivedFrame(frame, message.receivedAtNanos());
        }
    }

    CompletableFuture<Void> sendInput(
        int protocolVersion,
        long targetFrame,
        long sequence,
        byte[] payload
    ) {
        Envelope envelope = Envelope.newBuilder()
            .setProtocolVersion(protocolVersion)
            .setRequestId("input-" + playerId + "-" + sequence)
            .setClientInput(ClientInput.newBuilder()
                .setTargetFrame((int) targetFrame)
                .setSequence((int) sequence)
                .setPayload(ByteString.copyFrom(payload)))
            .build();
        return send(envelope);
    }

    void startHeartbeat(int protocolVersion, long intervalMillis) {
        if (intervalMillis <= 0 || heartbeat != null) {
            throw new IllegalArgumentException("无效或重复的心跳配置");
        }
        heartbeat = heartbeatExecutor.scheduleAtFixedRate(
            () -> {
                long sequence = pingSequence.updateAndGet(
                    current -> current == 0xffff_ffffL ? 1 : current + 1
                );
                Envelope ping = Envelope.newBuilder()
                    .setProtocolVersion(protocolVersion)
                    .setRequestId("ping-" + playerId + "-" + sequence)
                    .setClientPing(ClientPing.newBuilder().setSequence((int) sequence))
                    .build();
                send(ping).whenComplete((ignored, error) -> {
                    if (error != null) {
                        incoming.offer(unwrap(error));
                    }
                });
            },
            intervalMillis,
            intervalMillis,
            TimeUnit.MILLISECONDS
        );
    }

    private CompletableFuture<Void> send(Envelope envelope) {
        synchronized (sendLock) {
            sendTail = sendTail.thenCompose(ignored -> {
                WebSocket current = socket;
                if (current == null || current.isOutputClosed()) {
                    return CompletableFuture.failedFuture(
                        new IllegalStateException(
                            "玩家 " + playerId + " 的 WebSocket 已关闭"
                        )
                    );
                }
                return current.sendBinary(
                    ByteBuffer.wrap(envelope.toByteArray()),
                    true
                ).thenApply(sent -> null);
            });
            return sendTail;
        }
    }

    private void sendAndWait(Envelope envelope) throws Exception {
        send(envelope).get(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private TimedEnvelope take(long deadlineNanos) throws Exception {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
            throw timeout();
        }
        Object value = incoming.poll(remaining, TimeUnit.NANOSECONDS);
        if (value == null) {
            throw timeout();
        }
        if (value instanceof Throwable error) {
            throw new IllegalStateException(
                "玩家 " + playerId + " 的 WebSocket 发生异常",
                error
            );
        }
        return (TimedEnvelope) value;
    }

    private TimeoutException timeout() {
        return new TimeoutException(
            "等待玩家 " + playerId + " 的 WebSocket 消息超时"
        );
    }

    private void rejectProtocolError(Envelope envelope) {
        if (!envelope.hasProtocolError()) {
            return;
        }
        throw new IllegalStateException(
            "玩家 " + playerId + " 收到协议错误 "
                + envelope.getProtocolError().getCode() + "："
                + envelope.getProtocolError().getMessage()
        );
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        socket = webSocket;
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onBinary(
        WebSocket webSocket,
        ByteBuffer data,
        boolean last
    ) {
        byte[] fragment = new byte[data.remaining()];
        data.get(fragment);
        synchronized (binaryMessage) {
            binaryMessage.writeBytes(fragment);
            if (last) {
                try {
                    Envelope envelope = Envelope.parseFrom(
                        binaryMessage.toByteArray()
                    );
                    incoming.offer(new TimedEnvelope(envelope, System.nanoTime()));
                } catch (InvalidProtocolBufferException error) {
                    incoming.offer(error);
                } finally {
                    binaryMessage.reset();
                }
            }
        }
        webSocket.request(1);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<?> onText(
        WebSocket webSocket,
        CharSequence data,
        boolean last
    ) {
        incoming.offer(new IllegalStateException(
            "收到非预期的文本 WebSocket 消息"
        ));
        webSocket.request(1);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<?> onClose(
        WebSocket webSocket,
        int statusCode,
        String reason
    ) {
        incoming.offer(new IllegalStateException(
            "WebSocket 已关闭，状态码 " + statusCode + "，原因：" + reason
        ));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        incoming.offer(error);
    }

    @Override
    public void close() {
        if (heartbeat != null) {
            heartbeat.cancel(false);
        }
        heartbeatExecutor.shutdownNow();
        WebSocket current = socket;
        if (current == null || current.isOutputClosed()) {
            return;
        }
        try {
            current.sendClose(WebSocket.NORMAL_CLOSURE, "test client cleanup")
                .get(1, TimeUnit.SECONDS);
        } catch (Exception error) {
            current.abort();
        }
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException
            || current instanceof java.util.concurrent.ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    record ReceivedFrame(ServerFrame frame, long receivedAtNanos) {
    }

    private record TimedEnvelope(Envelope envelope, long receivedAtNanos) {
    }
}
