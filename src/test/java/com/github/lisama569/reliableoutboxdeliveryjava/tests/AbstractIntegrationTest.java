package com.github.lisama569.reliableoutboxdeliveryjava.tests;

import com.github.lisama569.reliableoutboxdeliveryjava.outbox.OutboxSchedulerConfig;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.FailureSimulator;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.MessageQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.jdbc.JdbcTestUtils;

/**
 * 集成测试基类：每个测试方法开始前暂停投递器、清理业务表并重置故障注入，
 * 使各测试在共享 Spring 上下文的情况下互不干扰，并允许测试精确控制投递时序。
 */
public abstract class AbstractIntegrationTest {

    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    protected FailureSimulator failureSimulator;
    @Autowired
    private MessageQueue messageQueue;
    @Autowired
    protected OutboxSchedulerConfig dispatcherGroup;

    @BeforeEach
    void resetState() {
        dispatcherGroup.pauseAll();
        JdbcTestUtils.deleteFromTables(jdbc, "dispatch_audit", "processed_event",
                "fulfillment", "biz_order", "outbox_event");
        failureSimulator.reset();
        messageQueue.clearAll();
    }

    @AfterEach
    void resumeAfterTest() {
        failureSimulator.reset();
        dispatcherGroup.resumeAll();
    }

    protected void resumeDispatchers() {
        dispatcherGroup.resumeAll();
    }
}
