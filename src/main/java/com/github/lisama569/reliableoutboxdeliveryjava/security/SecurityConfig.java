package com.github.lisama569.reliableoutboxdeliveryjava.security;

import jakarta.servlet.http.HttpServletResponse;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/**
 * HTTP Basic 认证 + 基于角色的授权（本地模拟，无外部 IAM 依赖）。
 * 角色：
 * - OPS    运维：业务操作 + 事件/死信查询 + 死信重投/标记已处理
 * - VIEWER 只读：业务与事件查询，不能执行任何状态变更操作
 * 未认证 -> 401；已认证但角色不足 -> 403，响应体包含可区分的错误码。
 */
@Configuration
@EnableMethodSecurity(securedEnabled = true)
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http.csrf(csrf -> csrf.disable())
                .authorizeHttpRequests(auth -> auth.anyRequest().authenticated())
                .httpBasic(basic -> {
                })
                .exceptionHandling(eh -> eh
                        .authenticationEntryPoint((request, response, authException) -> {
                            response.setStatus(HttpStatus.UNAUTHORIZED.value());
                            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                            response.getWriter().write(
                                    "{\"status\":401,\"code\":\"UNAUTHENTICATED\","
                                            + "\"message\":\"authentication required\"}");
                        })
                        .accessDeniedHandler((request, response, accessDeniedException) -> {
                            response.setStatus(HttpStatus.FORBIDDEN.value());
                            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                            response.getWriter().write(
                                    "{\"status\":403,\"code\":\"ACCESS_DENIED\","
                                            + "\"message\":\"insufficient role\"}");
                        }));
        return http.build();
    }

    @Bean
    public UserDetailsService userDetailsService() {
        var ops = User.withUsername("ops")
                .password("ops-pass")
                .roles("OPS")
                .build();
        var viewer = User.withUsername("viewer")
                .password("viewer-pass")
                .roles("VIEWER")
                .build();
        return new InMemoryUserDetailsManager(ops, viewer);
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return NoOpPasswordEncoder.getInstance();
    }
}
