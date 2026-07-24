package com.rainnov.lockstep.api;

import com.rainnov.lockstep.api.dto.NodeCapacityResponse;
import com.rainnov.lockstep.api.dto.PlayerRequest;
import com.rainnov.lockstep.api.dto.RoomAllocationRequest;
import com.rainnov.lockstep.api.dto.RoomAllocationResponse;
import com.rainnov.lockstep.api.dto.RoomTerminationRequest;
import com.rainnov.lockstep.api.dto.RoomTerminationResponse;
import com.rainnov.lockstep.room.CapacitySnapshot;
import com.rainnov.lockstep.room.MatchPhase;
import com.rainnov.lockstep.room.RoomPoolManager;
import com.rainnov.lockstep.room.RoomState;
import com.rainnov.lockstep.room.TerminationMode;
import com.rainnov.lockstep.room.TerminationReason;
import com.rainnov.lockstep.security.ApiKeyWebFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "lockstep.node.id=control-plane-test-node",
        "lockstep.node.shutdown-grace=0s",
        "lockstep.pool.target-size=1",
        "lockstep.pool.room-executor-threads=1",
        "lockstep.pool.health-check-interval=1h",
        "lockstep.room.join-timeout=2m",
        "lockstep.room.max-duration=5m",
        "lockstep.data-plane.port=0",
        "lockstep.data-plane.advertised-uri=ws://data.example.test/game",
        "lockstep.security.api-key=control-plane-test-api-key",
        "lockstep.security.ticket-secret=control-plane-test-ticket-secret"
    }
)
@Execution(ExecutionMode.SAME_THREAD)
class ControlPlaneIntegrationTest {

    private static final String API_KEY = "control-plane-test-api-key";

    @LocalServerPort
    private int controlPort;

    @Autowired
    private RoomPoolManager roomPool;

    private final Map<String, String> allocatedRooms = new LinkedHashMap<>();
    private WebTestClient client;

    @BeforeEach
    void createClientAndWaitForCapacity() {
        client = WebTestClient.bindToServer()
            .baseUrl("http://127.0.0.1:" + controlPort)
            .responseTimeout(Duration.ofSeconds(5))
            .build();
        awaitReadyRoom();
    }

    @AfterEach
    void releaseAllocatedRooms() {
        for (Map.Entry<String, String> allocation : allocatedRooms.entrySet()) {
            try {
                roomPool.terminate(
                    allocation.getKey(),
                    allocation.getValue(),
                    TerminationMode.FORCE,
                    TerminationReason.ADMINISTRATIVE
                ).toCompletableFuture().get(2, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // 测试可能已经终止这个一次性房间。
            }
        }
        allocatedRooms.clear();
        awaitReadyRoom();
    }

    @Test
    void requiresApiKeyForBusinessAndInternalEndpoints() {
        client.get()
            .uri("/internal/v1/node/capacity")
            .exchange()
            .expectStatus().isUnauthorized()
            .expectHeader().exists(RequestIdWebFilter.HEADER)
            .expectBody()
            .jsonPath("$.code").isEqualTo("UNAUTHORIZED")
            .jsonPath("$.message").isEqualTo("A valid X-API-Key header is required")
            .jsonPath("$.requestId").isNotEmpty()
            .jsonPath("$.timestamp").isNotEmpty();

        client.get()
            .uri("/api/v1/rooms/unknown-room")
            .header(ApiKeyWebFilter.HEADER, "wrong-key")
            .exchange()
            .expectStatus().isUnauthorized()
            .expectBody()
            .jsonPath("$.code").isEqualTo("UNAUTHORIZED");
    }

    @Test
    void exposesHealthProbesAndProtectsMetrics() {
        client.get()
            .uri("/actuator/health/liveness")
            .exchange()
            .expectStatus().isOk()
            .expectBody()
            .jsonPath("$.status").isEqualTo("UP");

        client.get()
            .uri("/actuator/health/readiness")
            .exchange()
            .expectStatus().isOk()
            .expectBody()
            .jsonPath("$.status").isEqualTo("UP");

        client.get()
            .uri("/actuator/metrics")
            .exchange()
            .expectStatus().isUnauthorized()
            .expectBody()
            .jsonPath("$.code").isEqualTo("UNAUTHORIZED");

        client.get()
            .uri("/actuator/metrics")
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .exchange()
            .expectStatus().isOk()
            .expectBody()
            .jsonPath("$.names").isArray();
    }

    @Test
    void validatesAllocationPayloadAndRequiredIdempotencyHeader() {
        client.post()
            .uri("/api/v1/room-allocations")
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .header("Idempotency-Key", "invalid-allocation")
            .header(RequestIdWebFilter.HEADER, "validation-request")
            .bodyValue(new RoomAllocationRequest(" ", List.of()))
            .exchange()
            .expectStatus().isBadRequest()
            .expectHeader().valueEquals(RequestIdWebFilter.HEADER, "validation-request")
            .expectBody()
            .jsonPath("$.code").isEqualTo("INVALID_REQUEST")
            .jsonPath("$.requestId").isEqualTo("validation-request")
            .jsonPath("$.message").isNotEmpty()
            .jsonPath("$.timestamp").isNotEmpty();

        client.post()
            .uri("/api/v1/room-allocations")
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .bodyValue(request("missing-header-match", "player-1"))
            .exchange()
            .expectStatus().isBadRequest()
            .expectBody()
            .jsonPath("$.code").isEqualTo("INVALID_REQUEST");

        client.post()
            .uri("/api/v1/room-allocations")
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .header("Idempotency-Key", "unsafe-player")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("""
                {"matchId":"safe-match","players":[{"playerId":" player-1 "}]}
                """)
            .exchange()
            .expectStatus().isBadRequest()
            .expectBody()
            .jsonPath("$.code").isEqualTo("INVALID_REQUEST");

        client.post()
            .uri("/api/v1/room-allocations")
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .header("Idempotency-Key", "null-player")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("""
                {"matchId":"safe-match","players":[null]}
                """)
            .exchange()
            .expectStatus().isBadRequest()
            .expectBody()
            .jsonPath("$.code").isEqualTo("INVALID_REQUEST");

        awaitReadyRoom();
    }

    @Test
    void replaysIdenticalIdempotentAllocationAndRejectsChangedRequest() {
        RoomAllocationRequest original = request(
            "idempotent-match",
            "player-1",
            "player-2"
        );
        RoomAllocationResponse first = allocate(
            "stable-idempotency-key",
            original,
            HttpStatus.CREATED
        );
        RoomAllocationResponse replay = allocate(
            "stable-idempotency-key",
            original,
            HttpStatus.OK
        );

        assertThat(replay.allocationId()).isEqualTo(first.allocationId());
        assertThat(replay.roomId()).isEqualTo(first.roomId());
        assertThat(replay.players()).isEqualTo(first.players());
        assertThat(replay.dataPlaneEndpoints()).isEqualTo(first.dataPlaneEndpoints());

        client.post()
            .uri("/api/v1/room-allocations")
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .header("Idempotency-Key", "stable-idempotency-key")
            .bodyValue(request("different-match", "player-1", "player-2"))
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.CONFLICT)
            .expectBody()
            .jsonPath("$.code").isEqualTo("IDEMPOTENCY_CONFLICT")
            .jsonPath("$.requestId").isNotEmpty()
            .jsonPath("$.timestamp").isNotEmpty();
    }

    @Test
    void returnsRetryAfterWhenNoReadyRoomIsAvailable() {
        allocate(
            "capacity-owner-key",
            request("capacity-owner-match", "player-1"),
            HttpStatus.CREATED
        );

        client.post()
            .uri("/api/v1/room-allocations")
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .header("Idempotency-Key", "capacity-rejected-key")
            .bodyValue(request("capacity-rejected-match", "player-2"))
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
            .expectHeader().valueEquals("Retry-After", "1")
            .expectBody()
            .jsonPath("$.code").isEqualTo("ROOM_CAPACITY_EXHAUSTED")
            .jsonPath("$.requestId").isNotEmpty()
            .jsonPath("$.timestamp").isNotEmpty();
    }

    @Test
    void queriesRoomStateWithoutExposingConnectionTickets() {
        RoomAllocationResponse allocation = allocate(
            "room-query-key",
            request("room-query-match", "player-a", "player-b"),
            HttpStatus.CREATED
        );

        client.get()
            .uri("/api/v1/rooms/{roomId}", allocation.roomId())
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .exchange()
            .expectStatus().isOk()
            .expectBody()
            .jsonPath("$.nodeId").isEqualTo("control-plane-test-node")
            .jsonPath("$.roomId").isEqualTo(allocation.roomId())
            .jsonPath("$.matchId").isEqualTo("room-query-match")
            .jsonPath("$.state").isEqualTo(RoomState.ACTIVE.name())
            .jsonPath("$.matchPhase").isEqualTo(MatchPhase.WAITING_FOR_PLAYERS.name())
            .jsonPath("$.currentFrame").isEqualTo(0)
            .jsonPath("$.players.length()").isEqualTo(2)
            .jsonPath("$.players[0].playerId").isEqualTo("player-a")
            .jsonPath("$.players[0].ticket").doesNotExist()
            .jsonPath("$.ticket").doesNotExist();
    }

    @Test
    void terminatesRoomIdempotentlyAndKeepsTerminalSnapshotQueryable() {
        RoomAllocationRequest request = request("termination-match", "player-1");
        RoomAllocationResponse allocation = allocate(
            "termination-key",
            request,
            HttpStatus.CREATED
        );
        RoomTerminationRequest termination = new RoomTerminationRequest(
            "termination-match",
            TerminationMode.GRACEFUL,
            TerminationReason.MATCH_COMPLETED
        );

        RoomTerminationResponse first = terminate(allocation.roomId(), termination);
        RoomTerminationResponse replay = terminate(allocation.roomId(), termination);

        assertThat(first).isEqualTo(replay);
        assertThat(first.roomStatus()).isEqualTo(RoomState.TERMINATED);
        assertThat(first.mode()).isEqualTo(TerminationMode.GRACEFUL);
        assertThat(first.reason()).isEqualTo(TerminationReason.MATCH_COMPLETED);

        RoomAllocationResponse allocationReplay = allocate(
            "termination-key",
            request,
            HttpStatus.OK
        );
        assertThat(allocationReplay.allocationId()).isEqualTo(allocation.allocationId());
        assertThat(allocationReplay.roomId()).isEqualTo(allocation.roomId());
        assertThat(allocationReplay.players()).isEqualTo(allocation.players());

        client.get()
            .uri("/api/v1/rooms/{roomId}", allocation.roomId())
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .exchange()
            .expectStatus().isOk()
            .expectBody()
            .jsonPath("$.state").isEqualTo(RoomState.TERMINATED.name())
            .jsonPath("$.matchPhase").isEqualTo(MatchPhase.FINISHED.name())
            .jsonPath("$.terminationMode").isEqualTo(TerminationMode.GRACEFUL.name())
            .jsonPath("$.terminationReason")
            .isEqualTo(TerminationReason.MATCH_COMPLETED.name());
    }

    @Test
    void exposesCurrentNodeCapacityAndAdvertisedDataPlaneEndpoint() {
        NodeCapacityResponse capacity = client.get()
            .uri("/internal/v1/node/capacity")
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .exchange()
            .expectStatus().isOk()
            .expectBody(NodeCapacityResponse.class)
            .returnResult()
            .getResponseBody();

        assertThat(capacity).isNotNull();
        assertThat(capacity.nodeId()).isEqualTo("control-plane-test-node");
        assertThat(capacity.nodeStatus().name()).isEqualTo("READY");
        assertThat(capacity.targetRooms()).isEqualTo(1);
        assertThat(capacity.readyRooms()).isEqualTo(1);
        assertThat(capacity.healthyRooms()).isEqualTo(1);
        assertThat(capacity.totalLiveRooms()).isEqualTo(1);
        assertThat(capacity.dataPlaneEndpoints()).singleElement().satisfies(endpoint -> {
            assertThat(endpoint.transport()).isEqualTo("WEBSOCKET");
            assertThat(endpoint.uri()).isEqualTo("ws://data.example.test/game");
            assertThat(endpoint.subprotocol()).isEqualTo("lockstep.protobuf.v1");
            assertThat(endpoint.encoding()).isEqualTo("PROTOBUF");
        });
        assertThat(capacity.sampledAt()).isNotNull();
    }

    private RoomAllocationResponse allocate(
        String idempotencyKey,
        RoomAllocationRequest request,
        HttpStatus expectedStatus
    ) {
        RoomAllocationResponse response = client.post()
            .uri("/api/v1/room-allocations")
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .header("Idempotency-Key", idempotencyKey)
            .bodyValue(request)
            .exchange()
            .expectStatus().isEqualTo(expectedStatus)
            .expectBody(RoomAllocationResponse.class)
            .returnResult()
            .getResponseBody();
        assertThat(response).isNotNull();
        allocatedRooms.put(response.roomId(), response.matchId());
        return response;
    }

    private RoomTerminationResponse terminate(
        String roomId,
        RoomTerminationRequest request
    ) {
        RoomTerminationResponse response = client.post()
            .uri("/api/v1/rooms/{roomId}/termination", roomId)
            .header(ApiKeyWebFilter.HEADER, API_KEY)
            .bodyValue(request)
            .exchange()
            .expectStatus().isAccepted()
            .expectBody(RoomTerminationResponse.class)
            .returnResult()
            .getResponseBody();
        assertThat(response).isNotNull();
        return response;
    }

    private void awaitReadyRoom() {
        await()
            .atMost(Duration.ofSeconds(5))
            .pollInterval(Duration.ofMillis(20))
            .untilAsserted(() -> {
                CapacitySnapshot capacity = roomPool.capacity()
                    .toCompletableFuture()
                    .get(1, TimeUnit.SECONDS);
                assertThat(capacity.acceptingAllocations()).isTrue();
                assertThat(capacity.readyRooms()).isEqualTo(1);
                assertThat(capacity.totalLiveRooms()).isEqualTo(1);
            });
    }

    private static RoomAllocationRequest request(String matchId, String... playerIds) {
        return new RoomAllocationRequest(
            matchId,
            java.util.Arrays.stream(playerIds).map(PlayerRequest::new).toList()
        );
    }
}
