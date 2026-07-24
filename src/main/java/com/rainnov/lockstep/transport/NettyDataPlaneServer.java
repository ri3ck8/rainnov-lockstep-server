package com.rainnov.lockstep.transport;

import com.rainnov.lockstep.config.LockstepProperties;
import com.rainnov.lockstep.node.NodeLifecycleManager;
import com.rainnov.lockstep.room.RoomPoolManager;
import com.rainnov.lockstep.security.ticket.TicketService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dedicated Netty server for the WebSocket game data plane.
 */
@Component
public final class NettyDataPlaneServer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(NettyDataPlaneServer.class);
    private static final Duration EVENT_LOOP_SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);

    private final Object lifecycleMonitor = new Object();
    private final LockstepProperties properties;
    private final RoomCommandGateway rooms;
    private final TicketService tickets;
    private final NodeLifecycleManager nodeLifecycle;
    private final RoomPoolManager roomPool;
    private final DataPlaneTelemetry telemetry;

    private volatile boolean running;
    private volatile Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public NettyDataPlaneServer(
        LockstepProperties properties,
        RoomCommandGateway rooms,
        TicketService tickets,
        NodeLifecycleManager nodeLifecycle,
        RoomPoolManager roomPool,
        DataPlaneTelemetry telemetry
    ) {
        this.properties = Objects.requireNonNull(properties, "properties");
        this.rooms = Objects.requireNonNull(rooms, "rooms");
        this.tickets = Objects.requireNonNull(tickets, "tickets");
        this.nodeLifecycle = Objects.requireNonNull(nodeLifecycle, "nodeLifecycle");
        this.roomPool = Objects.requireNonNull(roomPool, "roomPool");
        this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
    }

    @Override
    public void start() {
        synchronized (lifecycleMonitor) {
            if (running) {
                return;
            }

            EventLoopGroup newBoss = new NioEventLoopGroup(
                1,
                new DefaultThreadFactory("lockstep-data-boss")
            );
            EventLoopGroup newWorkers = new NioEventLoopGroup(
                0,
                new DefaultThreadFactory("lockstep-data-worker")
            );
            try {
                LockstepProperties.DataPlane dataPlane = properties.getDataPlane();
                ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(newBoss, newWorkers)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(
                        ChannelOption.WRITE_BUFFER_WATER_MARK,
                        new WriteBufferWaterMark(
                            dataPlane.getWriteBufferLowWaterMark(),
                            dataPlane.getWriteBufferHighWaterMark()
                        )
                    )
                    .childHandler(channelInitializer(dataPlane));

                ChannelFuture bind = bootstrap.bind(dataPlane.getPort()).syncUninterruptibly();
                if (!bind.isSuccess()) {
                    throw new IllegalStateException(
                        "Could not bind Netty data-plane server",
                        bind.cause()
                    );
                }
                bossGroup = newBoss;
                workerGroup = newWorkers;
                serverChannel = bind.channel();
                running = true;
                Channel boundChannel = bind.channel();
                boundChannel.closeFuture().addListener(ignored -> {
                    synchronized (lifecycleMonitor) {
                        if (serverChannel == boundChannel) {
                            running = false;
                            log.error("Netty data-plane listener closed unexpectedly");
                            nodeLifecycle.beginDraining();
                            roomPool.drain(Duration.ZERO).whenComplete((unused, error) -> {
                                if (error != null) {
                                    log.error(
                                        "Failed to drain rooms after data-plane listener loss",
                                        error
                                    );
                                }
                                nodeLifecycle.markTerminated();
                            });
                        }
                    }
                });
                log.info(
                    "Netty data plane listening on port {} at path {} with subprotocol {}",
                    ((InetSocketAddress) boundChannel.localAddress()).getPort(),
                    dataPlane.getPath(),
                    dataPlane.getSubprotocol()
                );
            } catch (RuntimeException error) {
                shutdownEventLoop(newBoss);
                shutdownEventLoop(newWorkers);
                throw error;
            }
        }
    }

    private ChannelInitializer<SocketChannel> channelInitializer(
        LockstepProperties.DataPlane dataPlane
    ) {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(SocketChannel channel) {
                channel.pipeline()
                    .addLast("http-codec", new HttpServerCodec())
                    .addLast(
                        "http-aggregator",
                        new HttpObjectAggregator(dataPlane.getMaxWebsocketFrameBytes())
                    )
                    .addLast(
                        "websocket-protocol",
                        new WebSocketServerProtocolHandler(
                            WebSocketServerProtocolConfig.newBuilder()
                                .websocketPath(dataPlane.getPath())
                                .subprotocols(dataPlane.getSubprotocol())
                                .allowExtensions(false)
                                .maxFramePayloadLength(
                                    dataPlane.getMaxWebsocketFrameBytes()
                                )
                                .build()
                        )
                    )
                    .addLast(
                        "websocket-frame-aggregator",
                        new WebSocketFrameAggregator(dataPlane.getMaxWebsocketFrameBytes())
                    )
                    .addLast(
                        "lockstep-protocol",
                        new WebSocketDataPlaneHandler(
                            rooms,
                            tickets,
                            properties,
                            telemetry
                        )
                    );
            }
        };
    }

    @Override
    public void stop() {
        stop(() -> {
            // The callback overload is used by Spring for ordered shutdown.
        });
    }

    @Override
    public void stop(Runnable callback) {
        Objects.requireNonNull(callback, "callback");
        Channel channel;
        EventLoopGroup boss;
        EventLoopGroup workers;
        synchronized (lifecycleMonitor) {
            if (!running && serverChannel == null && bossGroup == null && workerGroup == null) {
                callback.run();
                return;
            }
            running = false;
            channel = serverChannel;
            boss = bossGroup;
            workers = workerGroup;
            serverChannel = null;
            bossGroup = null;
            workerGroup = null;
        }

        int operationCount = (channel == null ? 0 : 1)
            + (boss == null ? 0 : 1)
            + (workers == null ? 0 : 1);
        if (operationCount == 0) {
            callback.run();
            return;
        }
        AtomicInteger remaining = new AtomicInteger(operationCount);
        AtomicBoolean completed = new AtomicBoolean();
        Runnable operationCompleted = () -> {
            if (remaining.decrementAndGet() == 0 && completed.compareAndSet(false, true)) {
                log.info("Netty data plane stopped");
                callback.run();
            }
        };

        if (channel != null) {
            channel.close().addListener(ignored -> operationCompleted.run());
        }
        if (boss != null) {
            boss.shutdownGracefully(
                0,
                EVENT_LOOP_SHUTDOWN_TIMEOUT.toMillis(),
                TimeUnit.MILLISECONDS
            ).addListener(ignored -> operationCompleted.run());
        }
        if (workers != null) {
            workers.shutdownGracefully(
                0,
                EVENT_LOOP_SHUTDOWN_TIMEOUT.toMillis(),
                TimeUnit.MILLISECONDS
            ).addListener(ignored -> operationCompleted.run());
        }
    }

    @Override
    public boolean isRunning() {
        Channel channel = serverChannel;
        return running && channel != null && channel.isActive();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * Starts before the room-pool lifecycle (phase 100) and stops after it.
     */
    @Override
    public int getPhase() {
        return 0;
    }

    public InetSocketAddress localAddress() {
        Channel channel = serverChannel;
        if (channel == null || !(channel.localAddress() instanceof InetSocketAddress address)) {
            return null;
        }
        return address;
    }

    private static void shutdownEventLoop(EventLoopGroup group) {
        group.shutdownGracefully(
            0,
            EVENT_LOOP_SHUTDOWN_TIMEOUT.toMillis(),
            TimeUnit.MILLISECONDS
        );
    }
}
