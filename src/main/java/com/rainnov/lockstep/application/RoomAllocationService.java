package com.rainnov.lockstep.application;

import com.rainnov.lockstep.config.LockstepProperties;
import com.rainnov.lockstep.room.AllocationSnapshot;
import com.rainnov.lockstep.room.RoomException;
import com.rainnov.lockstep.room.RoomPoolManager;
import com.rainnov.lockstep.room.RoomSnapshot;
import com.rainnov.lockstep.security.IdentifierPolicy;
import com.rainnov.lockstep.security.ticket.TicketClaims;
import com.rainnov.lockstep.security.ticket.TicketService;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public final class RoomAllocationService implements AutoCloseable {

    private final Object monitor = new Object();
    private final RoomPoolManager roomPool;
    private final TicketService ticketService;
    private final Clock clock;
    private final Duration ticketTtl;
    private final Duration terminalRetention;
    private final Map<String, Entry> byIdempotencyKey = new LinkedHashMap<>();
    private final Map<String, String> matchToIdempotencyKey = new LinkedHashMap<>();
    private final Map<String, Entry> roomToEntry = new LinkedHashMap<>();
    private final ScheduledExecutorService retentionScheduler =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("allocation-retention-", 0).factory()
        );

    public RoomAllocationService(
        RoomPoolManager roomPool,
        TicketService ticketService,
        LockstepProperties properties,
        Clock clock
    ) {
        this.roomPool = roomPool;
        this.ticketService = ticketService;
        this.clock = clock;
        this.ticketTtl = properties.getRoom().getMaxDuration()
            .plus(properties.getRoom().getReconnectGrace())
            .plus(properties.getRoom().getJoinTimeout());
        this.terminalRetention = properties.getPool().getTombstoneRetention();
        roomPool.addTerminationListener(this::onRoomTerminated);
    }

    public CompletionStage<AllocationOutcome> allocate(
        String idempotencyKey,
        String matchId,
        List<String> orderedPlayerIds
    ) {
        String key = requireText(idempotencyKey, "Idempotency-Key");
        String checkedMatchId = requireIdentifier(matchId, "matchId");
        List<String> players = validatePlayerIds(orderedPlayerIds);
        String fingerprint = fingerprint(checkedMatchId, players);

        Entry entry;
        boolean created;
        synchronized (monitor) {
            Entry existing = byIdempotencyKey.get(key);
            if (existing != null) {
                if (!MessageDigest.isEqual(
                    existing.fingerprint.getBytes(StandardCharsets.US_ASCII),
                    fingerprint.getBytes(StandardCharsets.US_ASCII)
                )) {
                    throw new RoomException(
                        "IDEMPOTENCY_CONFLICT",
                        "Idempotency-Key was already used for a different request"
                    );
                }
                entry = existing;
                created = false;
            } else {
                String matchOwner = matchToIdempotencyKey.get(checkedMatchId);
                if (matchOwner != null) {
                    throw new RoomException(
                        "MATCH_ALREADY_ALLOCATED",
                        "Match is already associated with another allocation"
                    );
                }
                Instant issuedAt = clock.instant();
                entry = new Entry(
                    key,
                    fingerprint,
                    checkedMatchId,
                    issuedAt,
                    issuedAt.plus(ticketTtl)
                );
                byIdempotencyKey.put(key, entry);
                matchToIdempotencyKey.put(checkedMatchId, key);
                created = true;
                beginAllocation(entry, players);
            }
        }

        boolean outcomeCreated = created;
        return entry.allocation.thenApply(allocation ->
            toOutcome(allocation, entry, outcomeCreated)
        );
    }

    private void beginAllocation(Entry entry, List<String> players) {
        CompletionStage<AllocationSnapshot> allocationStage;
        try {
            allocationStage = roomPool.allocate(entry.matchId, players);
        } catch (Throwable error) {
            failEntry(entry, error);
            return;
        }
        allocationStage.whenComplete((allocation, error) -> {
            if (error != null) {
                failEntry(entry, error);
                return;
            }
            synchronized (monitor) {
                roomToEntry.put(allocation.roomId(), entry);
            }
            entry.allocation.complete(allocation);
        });
    }

    private void failEntry(Entry entry, Throwable error) {
        synchronized (monitor) {
            byIdempotencyKey.remove(entry.idempotencyKey, entry);
            matchToIdempotencyKey.remove(entry.matchId, entry.idempotencyKey);
        }
        entry.allocation.completeExceptionally(unwrap(error));
    }

    private AllocationOutcome toOutcome(
        AllocationSnapshot allocation,
        Entry entry,
        boolean created
    ) {
        List<PlayerTicket> tickets = new ArrayList<>(allocation.playerIds().size());
        for (String playerId : allocation.playerIds()) {
            TicketClaims claims = new TicketClaims(
                allocation.protocolVersion(),
                allocation.nodeId(),
                allocation.roomId(),
                allocation.matchId(),
                playerId,
                entry.issuedAt,
                entry.expiresAt
            );
            tickets.add(new PlayerTicket(
                playerId,
                ticketService.issue(claims),
                entry.expiresAt
            ));
        }
        return new AllocationOutcome(allocation, created, tickets);
    }

    private void onRoomTerminated(RoomSnapshot snapshot) {
        if (snapshot.roomId() == null) {
            return;
        }
        Entry entry;
        synchronized (monitor) {
            entry = roomToEntry.remove(snapshot.roomId());
            if (entry == null) {
                return;
            }
            entry.terminal = true;
        }
        retentionScheduler.schedule(
            () -> removeTerminalEntry(entry),
            terminalRetention.toMillis(),
            TimeUnit.MILLISECONDS
        );
    }

    private void removeTerminalEntry(Entry entry) {
        synchronized (monitor) {
            if (!entry.terminal) {
                return;
            }
            byIdempotencyKey.remove(entry.idempotencyKey, entry);
            matchToIdempotencyKey.remove(entry.matchId, entry.idempotencyKey);
        }
    }

    @Override
    public void close() {
        retentionScheduler.shutdownNow();
    }

    private static String fingerprint(String matchId, List<String> players) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(matchId.getBytes(StandardCharsets.UTF_8));
            for (String player : players) {
                digest.update((byte) 0);
                digest.update(Objects.requireNonNull(player, "playerId")
                    .getBytes(StandardCharsets.UTF_8));
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
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

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new RoomException("INVALID_REQUEST", name + " must not be blank");
        }
        return value;
    }

    private static String requireIdentifier(String value, String name) {
        try {
            return IdentifierPolicy.requireValid(name, value);
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw new RoomException("INVALID_REQUEST", exception.getMessage());
        }
    }

    private static List<String> validatePlayerIds(List<String> orderedPlayerIds) {
        if (orderedPlayerIds == null) {
            throw new RoomException("INVALID_REQUEST", "players must not be null");
        }
        List<String> players = new ArrayList<>(orderedPlayerIds.size());
        for (String playerId : orderedPlayerIds) {
            players.add(requireIdentifier(playerId, "playerId"));
        }
        return List.copyOf(players);
    }

    private static final class Entry {
        private final String idempotencyKey;
        private final String fingerprint;
        private final String matchId;
        private final Instant issuedAt;
        private final Instant expiresAt;
        private final CompletableFuture<AllocationSnapshot> allocation =
            new CompletableFuture<>();
        private volatile boolean terminal;

        private Entry(
            String idempotencyKey,
            String fingerprint,
            String matchId,
            Instant issuedAt,
            Instant expiresAt
        ) {
            this.idempotencyKey = idempotencyKey;
            this.fingerprint = fingerprint;
            this.matchId = matchId;
            this.issuedAt = issuedAt;
            this.expiresAt = expiresAt;
        }
    }
}
