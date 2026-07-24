package com.rainnov.lockstep.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record PlayerRequest(
    @NotBlank
    @Size(max = 256)
    @Pattern(
        regexp = "^(?!.*\\p{Cc})\\S(?:.*\\S)?$",
        message = "must not have surrounding whitespace or control characters"
    )
    String playerId
) {
}
