package com.rainnov.lockstep.transport;

/**
 * 与协议及房间行为相互独立的传输层事件接收器。
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
