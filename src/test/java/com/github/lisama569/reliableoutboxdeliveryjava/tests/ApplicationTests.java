package com.github.lisama569.reliableoutboxdeliveryjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * 上下文冒烟测试。使用独立内存库，避免触碰应用默认的文件库。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:context-smoke;DB_CLOSE_DELAY=-1"
})
public class ApplicationTests {

    @Test
    public void contextLoads() {
    }
}
