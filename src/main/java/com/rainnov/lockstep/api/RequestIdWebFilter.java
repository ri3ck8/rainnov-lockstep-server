package com.rainnov.lockstep.api;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public final class RequestIdWebFilter implements WebFilter {

    public static final String ATTRIBUTE = RequestIdWebFilter.class.getName() + ".requestId";
    public static final String HEADER = "X-Request-Id";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String supplied = exchange.getRequest().getHeaders().getFirst(HEADER);
        String requestId = StringUtils.hasText(supplied) && supplied.length() <= 128
            ? supplied
            : UUID.randomUUID().toString();

        exchange.getAttributes().put(ATTRIBUTE, requestId);
        exchange.getResponse().getHeaders().set(HEADER, requestId);
        return chain.filter(exchange);
    }

    public static String from(ServerWebExchange exchange) {
        return exchange.getAttributeOrDefault(ATTRIBUTE, "unknown");
    }
}
