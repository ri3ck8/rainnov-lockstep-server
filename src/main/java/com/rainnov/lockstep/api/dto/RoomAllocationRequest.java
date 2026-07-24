package com.rainnov.lockstep.api.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.util.List;

public record RoomAllocationRequest(
    @NotBlank
    @Size(max = 256)
    @Pattern(
        regexp = "^(?!.*\\p{Cc})\\S(?:.*\\S)?$",
        message = "must not have surrounding whitespace or control characters"
    )
    String matchId,

    @NotEmpty
    @Size(max = 128)
    List<@NotNull @Valid PlayerRequest> players
) {
}
