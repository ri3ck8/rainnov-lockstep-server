package com.rainnov.lockstep.api;

import com.rainnov.lockstep.room.RoomException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.ServerWebInputException;

import java.time.Instant;
import java.util.stream.Collectors;

@RestControllerAdvice
public final class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    ResponseEntity<ApiError> handleApi(ApiException exception, ServerWebExchange exchange) {
        if ("ROOM_CAPACITY_EXHAUSTED".equals(exception.code())) {
            return ResponseEntity.status(exception.status())
                .header("Retry-After", "1")
                .body(error(exception.code(), exception.getMessage(), exchange));
        }
        return response(exception.status(), exception.code(), exception.getMessage(), exchange);
    }

    @ExceptionHandler(RoomException.class)
    ResponseEntity<ApiError> handleRoom(RoomException exception, ServerWebExchange exchange) {
        HttpStatus status = switch (exception.code()) {
            case "ROOM_NOT_FOUND", "ALLOCATION_NOT_FOUND" -> HttpStatus.NOT_FOUND;
            case "ROOM_CAPACITY_EXHAUSTED", "POOL_NOT_STARTED", "NODE_DRAINING" ->
                HttpStatus.SERVICE_UNAVAILABLE;
            case "INVALID_REQUEST", "INVALID_PLAYERS", "PLAYER_NOT_RESERVED" ->
                HttpStatus.BAD_REQUEST;
            default -> HttpStatus.CONFLICT;
        };
        if (status == HttpStatus.SERVICE_UNAVAILABLE) {
            return ResponseEntity.status(status)
                .header("Retry-After", "1")
                .body(error(exception.code(), exception.getMessage(), exchange));
        }
        return response(status, exception.code(), exception.getMessage(), exchange);
    }

    @ExceptionHandler(WebExchangeBindException.class)
    ResponseEntity<ApiError> handleBinding(WebExchangeBindException exception, ServerWebExchange exchange) {
        String message = exception.getFieldErrors().stream()
            .map(error -> error.getField() + " " + error.getDefaultMessage())
            .collect(Collectors.joining("; "));
        return response(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", message, exchange);
    }

    @ExceptionHandler(ServerWebInputException.class)
    ResponseEntity<ApiError> handleInput(ServerWebInputException exception, ServerWebExchange exchange) {
        return response(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", exception.getReason(), exchange);
    }

    @ExceptionHandler(Throwable.class)
    ResponseEntity<ApiError> handleUnexpected(Throwable exception, ServerWebExchange exchange) {
        log.error("Unhandled control-plane error, requestId={}", RequestIdWebFilter.from(exchange), exception);
        return response(
            HttpStatus.INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "An unexpected server error occurred",
            exchange
        );
    }

    private ResponseEntity<ApiError> response(
        HttpStatus status,
        String code,
        String message,
        ServerWebExchange exchange
    ) {
        return ResponseEntity.status(status).body(error(code, message, exchange));
    }

    private ApiError error(String code, String message, ServerWebExchange exchange) {
        return new ApiError(
            code,
            message,
            RequestIdWebFilter.from(exchange),
            Instant.now()
        );
    }
}
