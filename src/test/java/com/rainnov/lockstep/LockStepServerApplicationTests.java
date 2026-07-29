package com.rainnov.lockstep;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
    "lockstep.data-plane.port=0",
    "lockstep.pool.target-size=2",
    "lockstep.node.id=context-test-node",
    "lockstep.security.api-key=context-test-api-key",
    "lockstep.security.ticket-secret=context-test-ticket-secret"
})
class LockStepServerApplicationTests {

    @Test
    void contextLoads() {
    }

}
