package com.rainnov.lockstep.security;

import com.rainnov.lockstep.api.ApiError;
import com.rainnov.lockstep.api.RequestIdWebFilter;
import com.rainnov.lockstep.config.LockstepProperties;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public final class ApiKeyWebFilter implements WebFilter {

    public static final String HEADER = "X-API-Key";

    private final byte[] expectedApiKey;
    private final ObjectMapper objectMapper;

    public ApiKeyWebFilter(LockstepProperties properties, ObjectMapper objectMapper) {
        this.expectedApiKey = properties.getSecurity().getApiKey().getBytes(StandardCharsets.UTF_8);
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (!path.startsWith("/api/")
            && !path.startsWith("/internal/")
            && !path.startsWith("/actuator/metrics")) {
            return chain.filter(exchange);
        }

        String supplied = exchange.getRequest().getHeaders().getFirst(HEADER);
        byte[] actual = supplied == null
            ? new byte[0]
            : supplied.getBytes(StandardCharsets.UTF_8);
        if (MessageDigest.isEqual(expectedApiKey, actual)) {
            return chain.filter(exchange);
        }

        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        ApiError error = new ApiError(
            "UNAUTHORIZED",
            "A valid X-API-Key header is required",
            RequestIdWebFilter.from(exchange),
            Instant.now()
        );
        try {
            byte[] json = objectMapper.writeValueAsBytes(error);
            DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(json);
            return exchange.getResponse().writeWith(Mono.just(buffer));
        } catch (Exception serializationFailure) {
            return exchange.getResponse().setComplete();
        }
    }
}
