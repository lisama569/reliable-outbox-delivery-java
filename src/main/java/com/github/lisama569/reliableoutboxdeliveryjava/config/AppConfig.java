package com.github.lisama569.reliableoutboxdeliveryjava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(OutboxProperties.class)
public class AppConfig {
}
