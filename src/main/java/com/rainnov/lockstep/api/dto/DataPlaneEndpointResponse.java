package com.rainnov.lockstep.api.dto;

public record DataPlaneEndpointResponse(
    String transport,
    String uri,
    String subprotocol,
    String encoding
) {
}
