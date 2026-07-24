package com.rainnov.lockstep.node;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

@Component
public final class NodeLifecycleManager {

    private final AtomicReference<NodeState> state = new AtomicReference<>(NodeState.STARTING);

    public NodeState state() {
        return state.get();
    }

    public boolean isReady() {
        return state.get() == NodeState.READY;
    }

    public void markReady() {
        state.compareAndSet(NodeState.STARTING, NodeState.READY);
    }

    public void beginDraining() {
        state.getAndUpdate(current ->
            current == NodeState.TERMINATED ? NodeState.TERMINATED : NodeState.DRAINING
        );
    }

    public void markTerminated() {
        state.set(NodeState.TERMINATED);
    }
}
