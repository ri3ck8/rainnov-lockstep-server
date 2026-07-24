package com.rainnov.lockstep.testclient;

import com.rainnov.lockstep.protocol.PlayerFrameInput;
import com.rainnov.lockstep.protocol.ServerFrame;
import com.rainnov.lockstep.protocol.ServerHello;
import org.springframework.web.reactive.function.client.WebClientRequestException;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public final class LockstepTestClient {

    private LockstepTestClient() {
    }

    public static void main(String[] arguments) {
        if (Arrays.asList(arguments).contains("--help")
            || Arrays.asList(arguments).contains("-h")) {
            System.out.print(ClientOptions.usage());
            return;
        }
        try {
            ClientOptions options = ClientOptions.parse(arguments, System.getenv());
            VerificationResult result = new Runner(options).run();
            printResult(result);
        } catch (Throwable error) {
            Throwable cause = unwrap(error);
            System.err.println("[失败] " + cause.getMessage());
            if (!(cause instanceof IllegalArgumentException)
                && !(cause instanceof WebClientRequestException)) {
                cause.printStackTrace(System.err);
            }
            System.exit(1);
        }
    }

    private static void printResult(VerificationResult result) {
        System.out.println();
        System.out.println("[通过] 所有客户端的权威帧与本地模拟状态逐帧一致");
        System.out.println("  对局/房间："
            + result.matchId() + " / " + result.roomId());
        System.out.println("  客户端数量：" + result.playerCount());
        System.out.println("  验证帧范围：" + result.firstVerifiedFrame()
            + "-" + result.lastVerifiedFrame()
            + "（" + result.verifiedFrames() + " 帧）");
        System.out.println("  有效输入/空操作："
            + result.realInputs() + " / " + result.noOps());
        System.out.println("  权威帧流 SHA-256：" + result.frameStreamHash());
        System.out.println("  最终状态 SHA-256：" + result.stateHash());
        System.out.println("  最终坐标：" + result.positions());
        System.out.printf(
            Locale.ROOT,
            "  接收帧间隔：平均 %.2f ms，P95 %.2f ms，最大 %.2f ms（理论 %.2f ms）%n",
            result.averageIntervalMillis(),
            result.p95IntervalMillis(),
            result.maxIntervalMillis(),
            result.expectedIntervalMillis()
        );
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof java.util.concurrent.CompletionException
            || current instanceof java.util.concurrent.ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static final class Runner {

        private final ClientOptions options;
        private final ControlPlaneClient control;
        private final List<ProtobufWebSocketClient> clients = new ArrayList<>();
        private ControlPlaneClient.AllocationResponse allocation;

        private Runner(ClientOptions options) {
            this.options = options;
            this.control = new ControlPlaneClient(options);
        }

        private VerificationResult run() throws Exception {
            List<String> playerIds = IntStream.rangeClosed(1, options.playerCount())
                .mapToObj(index -> "test-player-" + index)
                .toList();
            try {
                System.out.println("[1/4] 申请测试房间：" + options.matchId());
                allocation = control.allocate(options.matchId(), playerIds);
                validateAllocation(playerIds);
                Endpoint endpoint = endpoint();
                System.out.println("[2/4] 连接 " + options.playerCount()
                    + " 个玩家到 " + endpoint.uri());
                connectPlayers(playerIds, endpoint);
                System.out.println("[3/4] 对局已开始，采集并模拟 "
                    + options.frameCount() + " 个有效输入帧");
                return verify(playerIds);
            } finally {
                cleanup();
            }
        }

        private void validateAllocation(List<String> playerIds) {
            if (allocation.protocolVersion() <= 0
                || allocation.tickRate() <= 0
                || allocation.maxLeadFrames() <= 0) {
                throw new IllegalStateException("房间分配响应包含无效的帧参数");
            }
            List<String> allocatedPlayers = allocation.players().stream()
                .map(ControlPlaneClient.PlayerTicket::playerId)
                .toList();
            if (!playerIds.equals(allocatedPlayers)) {
                throw new IllegalStateException(
                    "房间分配响应改变了测试玩家顺序"
                );
            }
        }

        private Endpoint endpoint() {
            ControlPlaneClient.DataPlaneEndpoint advertised =
                allocation.dataPlaneEndpoints().stream()
                    .filter(item -> "WEBSOCKET".equalsIgnoreCase(item.transport()))
                    .filter(item -> "PROTOBUF".equalsIgnoreCase(item.encoding()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                        "房间分配响应中没有 Protobuf WebSocket 数据面端点"
                    ));
            URI uri = options.dataUriOverride() == null
                ? URI.create(advertised.uri())
                : options.dataUriOverride();
            return new Endpoint(uri, advertised.subprotocol());
        }

        private void connectPlayers(List<String> playerIds, Endpoint endpoint)
            throws Exception {
            Map<String, ControlPlaneClient.PlayerTicket> tickets = new HashMap<>();
            for (ControlPlaneClient.PlayerTicket ticket : allocation.players()) {
                tickets.put(ticket.playerId(), ticket);
            }
            for (String playerId : playerIds) {
                ProtobufWebSocketClient client =
                    ProtobufWebSocketClient.connect(
                        playerId,
                        endpoint.uri(),
                        endpoint.subprotocol(),
                        options.messageTimeout()
                    );
                clients.add(client);
                ServerHello hello = client.authenticate(
                    allocation,
                    tickets.get(playerId)
                );
                validateHello(playerId, hello);
                client.startHeartbeat(
                    allocation.protocolVersion(),
                    Integer.toUnsignedLong(hello.getClientPingIntervalMs())
                );
            }
            for (ProtobufWebSocketClient client : clients) {
                client.awaitMatchStarted();
            }
        }

        private void validateHello(String playerId, ServerHello hello) {
            if (!playerId.equals(hello.getPlayerId())
                || !allocation.roomId().equals(hello.getRoomId())
                || !allocation.matchId().equals(hello.getMatchId())) {
                throw new IllegalStateException(
                    "玩家 " + playerId + " 的 ServerHello 标识不匹配"
                );
            }
            if (hello.getTickRate() != allocation.tickRate()
                || hello.getMaxLeadFrames() != allocation.maxLeadFrames()) {
                throw new IllegalStateException(
                    "玩家 " + playerId + " 的 ServerHello 帧参数不匹配"
                );
            }
        }

        private VerificationResult verify(List<String> playerIds) throws Exception {
            int leadFrames = allocation.maxLeadFrames();
            long firstVerifiedFrame = leadFrames + 1L;
            long lastVerifiedFrame = leadFrames + (long) options.frameCount();
            List<DeterministicReplica> replicas = IntStream
                .range(0, clients.size())
                .mapToObj(ignored -> new DeterministicReplica(playerIds))
                .toList();
            MessageDigest frameStreamDigest = DeterministicReplica.sha256();
            List<Double> intervalsMillis = new ArrayList<>();
            long[] lastArrival = new long[clients.size()];
            long realInputs = 0;
            long noOps = 0;
            int progressStep = Math.max(1, options.frameCount() / 4);

            for (long frameId = 1; frameId <= lastVerifiedFrame; frameId++) {
                List<ProtobufWebSocketClient.ReceivedFrame> received =
                    receiveFrameFromAll(frameId);
                assertSameAuthoritativeFrame(frameId, received);

                long targetFrame = frameId + leadFrames;
                if (targetFrame <= lastVerifiedFrame) {
                    sendInputs(targetFrame, leadFrames);
                }

                String expectedStateHash = null;
                for (int index = 0; index < received.size(); index++) {
                    ServerFrame frame = received.get(index).frame();
                    replicas.get(index).apply(frame);
                    String stateHash = replicas.get(index).stateHash();
                    if (expectedStateHash == null) {
                        expectedStateHash = stateHash;
                    } else if (!expectedStateHash.equals(stateHash)) {
                        throw new IllegalStateException(
                            "第 " + frameId + " 帧后客户端本地状态哈希不一致"
                        );
                    }
                }

                if (frameId < firstVerifiedFrame) {
                    continue;
                }
                ServerFrame authoritative = received.getFirst().frame();
                for (PlayerFrameInput input : authoritative.getInputsList()) {
                    if (input.getNoOp()) {
                        noOps++;
                    } else {
                        realInputs++;
                    }
                }
                if (noOps > 0) {
                    List<String> missingPlayers = authoritative.getInputsList()
                        .stream()
                        .filter(PlayerFrameInput::getNoOp)
                        .map(PlayerFrameInput::getPlayerId)
                        .toList();
                    throw new IllegalStateException(
                        "第 " + frameId + " 帧存在空操作，输入未及时到达的玩家："
                            + missingPlayers
                    );
                }
                byte[] frameBytes = authoritative.toByteArray();
                frameStreamDigest.update(ByteBuffer.allocate(Integer.BYTES)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putInt(frameBytes.length)
                    .array());
                frameStreamDigest.update(frameBytes);
                for (int index = 0; index < received.size(); index++) {
                    long arrivedAt = received.get(index).receivedAtNanos();
                    if (lastArrival[index] != 0) {
                        intervalsMillis.add(
                            (arrivedAt - lastArrival[index]) / 1_000_000.0
                        );
                    }
                    lastArrival[index] = arrivedAt;
                }
                long completed = frameId - firstVerifiedFrame + 1;
                if (completed % progressStep == 0
                    || frameId == lastVerifiedFrame) {
                    System.out.println("      已验证 " + completed + "/"
                        + options.frameCount() + " 帧");
                }
            }

            System.out.println("[4/4] 比较权威帧流、逐帧状态哈希与帧间隔");
            String stateHash = replicas.getFirst().stateHash();
            return new VerificationResult(
                allocation.matchId(),
                allocation.roomId(),
                clients.size(),
                firstVerifiedFrame,
                lastVerifiedFrame,
                options.frameCount(),
                realInputs,
                noOps,
                HexFormat.of().formatHex(frameStreamDigest.digest()),
                stateHash,
                replicas.getFirst().positions(),
                average(intervalsMillis),
                percentile(intervalsMillis, 0.95),
                intervalsMillis.stream()
                    .mapToDouble(Double::doubleValue)
                    .max()
                    .orElse(0),
                1000.0 / allocation.tickRate()
            );
        }

        private List<ProtobufWebSocketClient.ReceivedFrame> receiveFrameFromAll(
            long frameId
        ) throws Exception {
            List<ProtobufWebSocketClient.ReceivedFrame> received =
                new ArrayList<>(clients.size());
            for (ProtobufWebSocketClient client : clients) {
                received.add(client.awaitFrame(frameId));
            }
            return received;
        }

        private void assertSameAuthoritativeFrame(
            long frameId,
            List<ProtobufWebSocketClient.ReceivedFrame> received
        ) {
            byte[] expected = received.getFirst().frame().toByteArray();
            for (int index = 1; index < received.size(); index++) {
                byte[] actual = received.get(index).frame().toByteArray();
                if (!MessageDigest.isEqual(expected, actual)) {
                    throw new IllegalStateException(
                        "第 " + frameId + " 帧在客户端 1 与客户端 "
                            + (index + 1) + " 之间不一致"
                    );
                }
            }
        }

        private void sendInputs(long targetFrame, int leadFrames) throws Exception {
            long sequence = targetFrame - leadFrames;
            List<CompletableFuture<Void>> sends = new ArrayList<>(clients.size());
            for (int index = 0; index < clients.size(); index++) {
                sends.add(clients.get(index).sendInput(
                    allocation.protocolVersion(),
                    targetFrame,
                    sequence,
                    DeterministicReplica.command(index, targetFrame)
                ));
            }
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
                .get(
                    options.messageTimeout().toNanos(),
                    TimeUnit.NANOSECONDS
                );
        }

        private void cleanup() {
            if (allocation != null) {
                try {
                    control.terminate(allocation);
                } catch (RuntimeException error) {
                    System.err.println("[警告] 测试房间清理失败："
                        + unwrap(error).getMessage());
                }
            }
            clients.forEach(ProtobufWebSocketClient::close);
        }

        private static double average(List<Double> values) {
            return values.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0);
        }

        private static double percentile(List<Double> values, double percentile) {
            if (values.isEmpty()) {
                return 0;
            }
            List<Double> sorted = values.stream()
                .sorted(Comparator.naturalOrder())
                .toList();
            int index = Math.max(
                0,
                (int) Math.ceil(percentile * sorted.size()) - 1
            );
            return sorted.get(index);
        }
    }

    private record Endpoint(URI uri, String subprotocol) {
    }

    private record VerificationResult(
        String matchId,
        String roomId,
        int playerCount,
        long firstVerifiedFrame,
        long lastVerifiedFrame,
        int verifiedFrames,
        long realInputs,
        long noOps,
        String frameStreamHash,
        String stateHash,
        String positions,
        double averageIntervalMillis,
        double p95IntervalMillis,
        double maxIntervalMillis,
        double expectedIntervalMillis
    ) {
    }
}
