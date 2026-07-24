package com.rainnov.lockstep.room;

public record InputResult(
    InputDisposition disposition,
    long currentFrame,
    String message
) {
    public boolean accepted() {
        return disposition == InputDisposition.ACCEPTED
            || disposition == InputDisposition.DUPLICATE_IGNORED;
    }
}
