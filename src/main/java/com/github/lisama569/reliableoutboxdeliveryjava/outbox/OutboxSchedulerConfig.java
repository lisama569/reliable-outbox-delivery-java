package com.github.lisama569.reliableoutboxdeliveryjava.outbox;

import com.github.lisama569.reliableoutboxdeliveryjava.queue.MessageQueue;
import com.github.lisama569.reliableoutboxdeliveryjava.queue.QueueProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 根据 outbox.dispatchers 配置创建并管理多个相互独立、并发竞争的投递器。
 * 每个投递器拥有自己的调度线程，统一由本生命周期组件启动/停止。
 */
@Component
public class OutboxSchedulerConfig implements SmartLifecycle {

    private final List<OutboxDispatcher> dispatchers = new ArrayList<>();
    private volatile boolean running;

    public OutboxSchedulerConfig(OutboxProperties properties, OutboxRepository repository,
                                 MessageQueue messageQueue, QueueProperties queueProperties,
                                 JdbcTemplate jdbcTemplate) {
        for (int i = 1; i <= properties.dispatchers(); i++) {
            dispatchers.add(new OutboxDispatcher("dispatcher-" + i, properties, repository,
                    messageQueue, queueProperties, jdbcTemplate));
        }
    }

    @Override
    public synchronized void start() {
        if (!running) {
            dispatchers.forEach(OutboxDispatcher::start);
            running = true;
        }
    }

    @Override
    public synchronized void stop() {
        if (running) {
            dispatchers.forEach(OutboxDispatcher::stop);
            running = false;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public List<OutboxDispatcher> getDispatchers() {
        return List.copyOf(dispatchers);
    }

    /** 暂停全部投递器（测试用于精确控制事件注入时机）。 */
    public void pauseAll() {
        dispatchers.forEach(OutboxDispatcher::pausePolling);
    }

    public void resumeAll() {
        dispatchers.forEach(OutboxDispatcher::resumePolling);
    }
}
