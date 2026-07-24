package com.rainnov.lockstep.transport;

import com.rainnov.lockstep.protocol.Envelope;
import com.rainnov.lockstep.protocol.ServerPong;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.util.concurrent.DefaultEventExecutor;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class NettyDataPlaneSessionTest {

    @Test
    void writesEnvelopesAsBinaryFrames() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        NettyDataPlaneSession session =
            new NettyDataPlaneSession("session-1", channel);
        Envelope envelope = Envelope.newBuilder()
            .setProtocolVersion(1)
            .setServerPong(ServerPong.newBuilder().setSequence(7))
            .build();

        session.send(envelope);
        channel.runPendingTasks();

        BinaryWebSocketFrame frame = channel.readOutbound();
        try {
            Envelope decoded = Envelope.parseFrom(ByteBufUtil.getBytes(frame.content()));
            assertThat(decoded).isEqualTo(envelope);
            assertThat(session.sessionId()).isEqualTo("session-1");
        } finally {
            frame.release();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void sendsOneBoundedCloseFrameAndStopsAcceptingWrites() {
        EmbeddedChannel channel = new EmbeddedChannel();
        NettyDataPlaneSession session =
            new NettyDataPlaneSession("session-1", channel);
        String longReason = "连接已被较新的会话替换".repeat(30);

        session.close(4006, longReason);
        session.close(4007, "second close is ignored");
        channel.runPendingTasks();

        CloseWebSocketFrame frame = channel.readOutbound();
        try {
            assertThat(frame.statusCode()).isEqualTo(4006);
            assertThat(frame.reasonText().getBytes(StandardCharsets.UTF_8).length)
                .isLessThanOrEqualTo(123);
            assertThat(session.isWritable()).isFalse();
            assertThat((Object) channel.readOutbound()).isNull();
        } finally {
            frame.release();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void flushesMessagesAcceptedBeforeCloseInOrder() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        NettyDataPlaneSession session =
            new NettyDataPlaneSession("session-1", channel);
        DefaultEventExecutor caller = new DefaultEventExecutor();
        CountDownLatch submitted = new CountDownLatch(1);
        try {
            Envelope first = envelope("first");
            Envelope second = envelope("second");
            caller.execute(() -> {
                session.send(first);
                session.send(second);
                session.close(4000, "MATCH_COMPLETED");
                submitted.countDown();
            });

            assertThat(submitted.await(2, TimeUnit.SECONDS)).isTrue();
            channel.runPendingTasks();

            BinaryWebSocketFrame firstFrame = channel.readOutbound();
            BinaryWebSocketFrame secondFrame = channel.readOutbound();
            CloseWebSocketFrame closeFrame = channel.readOutbound();
            assertThat(Envelope.parseFrom(ByteBufUtil.getBytes(firstFrame.content())))
                .isEqualTo(first);
            assertThat(Envelope.parseFrom(ByteBufUtil.getBytes(secondFrame.content())))
                .isEqualTo(second);
            assertThat(closeFrame.statusCode()).isEqualTo(4000);
            firstFrame.release();
            secondFrame.release();
            closeFrame.release();
        } finally {
            caller.shutdownGracefully().syncUninterruptibly();
            channel.finishAndReleaseAll();
        }
    }

    private static Envelope envelope(String requestId) {
        return Envelope.newBuilder()
            .setProtocolVersion(1)
            .setRequestId(requestId)
            .build();
    }
}
