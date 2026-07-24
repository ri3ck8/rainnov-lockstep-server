package com.rainnov.lockstep.transport;

/**
 * Transport event sink kept separate from protocol/room behavior.
 */
public interface DataPlaneTelemetry {

    DataPlaneTelemetry NOOP = new DataPlaneTelemetry() {
        @Override
        public void recordHeartbeatTimeout() {
        }

        @Override
        public void recordReconnect() {
        }
    };

    void recordHeartbeatTimeout();

    void recordReconnect();
}
