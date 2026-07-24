package com.rainnov.lockstep.api;

import com.rainnov.lockstep.api.dto.DataPlaneEndpointResponse;
import com.rainnov.lockstep.api.dto.PlayerRequest;
import com.rainnov.lockstep.api.dto.PlayerTicketResponse;
import com.rainnov.lockstep.api.dto.RoomAllocationRequest;
import com.rainnov.lockstep.api.dto.RoomAllocationResponse;
import com.rainnov.lockstep.api.dto.RoomTerminationRequest;
import com.rainnov.lockstep.api.dto.RoomTerminationResponse;
import com.rainnov.lockstep.application.AllocationOutcome;
import com.rainnov.lockstep.application.RoomAllocationService;
import com.rainnov.lockstep.config.LockstepProperties;
import com.rainnov.lockstep.observability.RoomMetrics;
import com.rainnov.lockstep.room.AllocationSnapshot;
import com.rainnov.lockstep.room.RoomPoolManager;
import com.rainnov.lockstep.room.RoomSnapshot;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

@RestController
@RequestMapping("/api/v1")
public final class RoomController {

    private static final String IDEMPOTENCY_HEADER = "Idempotency-Key";

    private final RoomAllocationService allocationService;
    private final RoomPoolManager roomPool;
    private final LockstepProperties properties;
    private final RoomMetrics metrics;

    public RoomController(
        RoomAllocationService allocationService,
        RoomPoolManager roomPool,
        LockstepProperties properties,
        RoomMetrics metrics
    ) {
        this.allocationService = allocationService;
        this.roomPool = roomPool;
        this.properties = properties;
        this.metrics = metrics;
    }

    @PostMapping("/room-allocations")
    Mono<ResponseEntity<RoomAllocationResponse>> allocate(
        @RequestHeader(IDEMPOTENCY_HEADER) String idempotencyKey,
        @Valid @RequestBody RoomAllocationRequest request
    ) {
        List<String> playerIds = request.players().stream()
            .map(PlayerRequest::playerId)
            .toList();
        long activationStartedNanos = System.nanoTime();
        return Mono.defer(() -> Mono.fromCompletionStage(
            allocationService.allocate(idempotencyKey, request.matchId(), playerIds)
        )).map(outcome -> {
            if (outcome.created()) {
                metrics.recordAllocationSuccess(Duration.ofNanos(
                    Math.max(0, System.nanoTime() - activationStartedNanos)
                ));
            }
            HttpStatus status = outcome.created() ? HttpStatus.CREATED : HttpStatus.OK;
            return ResponseEntity.status(status).body(toResponse(outcome));
        }).onErrorResume(error -> {
            Throwable cause = unwrap(error);
            if (cause instanceof com.rainnov.lockstep.room.RoomException roomError
                && "ROOM_CAPACITY_EXHAUSTED".equals(roomError.code())) {
                metrics.recordCapacityExhausted();
                return Mono.error(new CapacityExhaustedException(roomError));
            }
            return Mono.error(cause);
        });
    }

    @GetMapping("/rooms/{roomId}")
    Mono<RoomSnapshot> room(@PathVariable String roomId) {
        return Mono.fromCompletionStage(roomPool.roomSnapshot(roomId));
    }

    @PostMapping("/rooms/{roomId}/termination")
    Mono<ResponseEntity<RoomTerminationResponse>> terminate(
        @PathVariable String roomId,
        @Valid @RequestBody RoomTerminationRequest request
    ) {
        return Mono.fromCompletionStage(roomPool.terminate(
            roomId,
            request.matchId(),
            request.mode(),
            request.reason()
        )).map(snapshot -> ResponseEntity.accepted().body(new RoomTerminationResponse(
            snapshot.roomId(),
            snapshot.matchId(),
            snapshot.state(),
            snapshot.terminationMode(),
            snapshot.terminationReason()
        )));
    }

    private RoomAllocationResponse toResponse(AllocationOutcome outcome) {
        AllocationSnapshot allocation = outcome.allocation();
        List<PlayerTicketResponse> tickets = outcome.playerTickets().stream()
            .map(ticket -> new PlayerTicketResponse(
                ticket.playerId(),
                ticket.token(),
                ticket.expiresAt()
            ))
            .toList();
        return new RoomAllocationResponse(
            allocation.allocationId(),
            allocation.nodeId(),
            allocation.roomId(),
            allocation.matchId(),
            allocation.roomState(),
            allocation.matchPhase(),
            allocation.protocolVersion(),
            allocation.tickRate(),
            allocation.inputDelayFrames(),
            allocation.maxLeadFrames(),
            properties.getDataPlane().getClientPingInterval().toMillis(),
            properties.getDataPlane().getConnectionIdleTimeout().toMillis(),
            properties.getRoom().getReconnectGrace().toMillis(),
            allocation.joinDeadline(),
            tickets,
            List.of(endpoint())
        );
    }

    private DataPlaneEndpointResponse endpoint() {
        return new DataPlaneEndpointResponse(
            "WEBSOCKET",
            properties.getDataPlane().getAdvertisedUri().toString(),
            properties.getDataPlane().getSubprotocol(),
            "PROTOBUF"
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

    static final class CapacityExhaustedException extends ApiException {
        private CapacityExhaustedException(Throwable cause) {
            super(
                HttpStatus.SERVICE_UNAVAILABLE,
                "ROOM_CAPACITY_EXHAUSTED",
                cause.getMessage()
            );
            initCause(cause);
        }
    }
}
