package com.rainnov.lockstep.api.dto;

import com.rainnov.lockstep.room.TerminationMode;
import com.rainnov.lockstep.room.TerminationReason;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record RoomTerminationRequest(
    @NotBlank
    @Size(max = 256)
    @Pattern(
        regexp = "^(?!.*\\p{Cc})\\S(?:.*\\S)?$",
        message = "must not have surrounding whitespace or control characters"
    )
    String matchId,

    @NotNull
    TerminationMode mode,

    @NotNull
    TerminationReason reason
) {
}
