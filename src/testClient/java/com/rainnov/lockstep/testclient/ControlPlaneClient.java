package com.rainnov.lockstep.testclient;

import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.Duration;
import java.util.List;

final class ControlPlaneClient {

    private static final String API_KEY_HEADER = "X-API-Key";
    private static final String IDEMPOTENCY_HEADER = "Idempotency-Key";

    private final WebClient webClient;
    private final Duration timeout;

    ControlPlaneClient(ClientOptions options) {
        String baseUrl = options.controlUrl().toString().replaceAll("/+$", "");
        this.webClient = WebClient.builder()
            .baseUrl(baseUrl)
            .defaultHeader(API_KEY_HEADER, options.apiKey())
            .build();
        this.timeout = options.messageTimeout();
    }

    AllocationResponse allocate(String matchId, List<String> playerIds) {
        AllocationRequest request = new AllocationRequest(
            matchId,
            playerIds.stream().map(PlayerRequest::new).toList()
        );
        try {
            AllocationResponse response = webClient.post()
                .uri("/api/v1/room-allocations")
                .header(IDEMPOTENCY_HEADER, "frame-sync-client-" + matchId)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .bodyToMono(AllocationResponse.class)
                .block(timeout);
            if (response == null) {
                throw new IllegalStateException("控制面返回了空的房间分配响应");
            }
            return response;
        } catch (WebClientResponseException error) {
            throw controlError("申请房间", error);
        }
    }

    void terminate(AllocationResponse allocation) {
        try {
            webClient.post()
                .uri(
                    "/api/v1/rooms/{roomId}/termination",
                    allocation.roomId()
                )
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new TerminationRequest(
                    allocation.matchId(),
                    "GRACEFUL",
                    "MATCH_COMPLETED"
                ))
                .retrieve()
                .toBodilessEntity()
                .block(timeout);
        } catch (WebClientResponseException error) {
            throw controlError("终止房间", error);
        }
    }

    private static IllegalStateException controlError(
        String operation,
        WebClientResponseException error
    ) {
        String response = error.getResponseBodyAsString();
        String details = response.isBlank() ? "" : "，响应：" + response;
        return new IllegalStateException(
            operation + "失败：HTTP " + error.getStatusCode().value() + details,
            error
        );
    }

    record AllocationRequest(String matchId, List<PlayerRequest> players) {
    }

    record PlayerRequest(String playerId) {
    }

    record TerminationRequest(String matchId, String mode, String reason) {
    }

    record AllocationResponse(
        String allocationId,
        String nodeId,
        String roomId,
        String matchId,
        String roomStatus,
        String matchPhase,
        int protocolVersion,
        int tickRate,
        int inputDelayFrames,
        int maxLeadFrames,
        long clientPingIntervalMillis,
        long connectionIdleTimeoutMillis,
        long reconnectGraceMillis,
        String joinDeadline,
        List<PlayerTicket> players,
        List<DataPlaneEndpoint> dataPlaneEndpoints
    ) {
    }

    record PlayerTicket(String playerId, String ticket, String expiresAt) {
    }

    record DataPlaneEndpoint(
        String transport,
        String uri,
        String subprotocol,
        String encoding
    ) {
    }
}
