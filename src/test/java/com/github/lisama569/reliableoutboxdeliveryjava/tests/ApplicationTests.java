package com.github.lisama569.reliableoutboxdeliveryjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 上下文加载冒烟测试。与其他集成测试使用相同的 web 环境，
 * 从而共享同一个 Spring 上下文缓存，避免多套投递器线程互相干扰。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApplicationTests extends AbstractIntegrationTest {

    @Test
    void contextLoads() {
    }
}
