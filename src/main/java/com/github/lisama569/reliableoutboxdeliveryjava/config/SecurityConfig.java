package com.github.lisama569.reliableoutboxdeliveryjava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.reliableoutboxdeliveryjava.web.ApiError;
import com.github.lisama569.reliableoutboxdeliveryjava.web.ErrorCode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

import java.nio.charset.StandardCharsets;

/**
 * HTTP Basic 鉴权（仅用于本地演示/测试，不落真实凭据）：
 * 业务接口匿名可用；事件查询需 OPS_VIEWER 或 OPS_ADMIN；
 * 死信重投/标记已处理仅 OPS_ADMIN。
 */
@Configuration
public class SecurityConfig {

    public static final String ROLE_ADMIN = "OPS_ADMIN";
    public static final String ROLE_VIEWER = "OPS_VIEWER";

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http,
                                                   ObjectMapper objectMapper) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/api/orders/**").permitAll()
                        .requestMatchers(org.springframework.http.HttpMethod.GET,
                                "/api/admin/events/**").hasAnyRole(ROLE_VIEWER, ROLE_ADMIN)
                        .requestMatchers("/api/admin/events/**").hasRole(ROLE_ADMIN)
                        .anyRequest().authenticated())
                .httpBasic(Customizer.withDefaults())
                .exceptionHandling(eh -> eh
                        .authenticationEntryPoint((request, response, authException) -> {
                            response.setStatus(401);
                            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                            response.setCharacterEncoding(StandardCharsets.UTF_8.name());
                            response.getWriter().write(objectMapper.writeValueAsString(
                                    ApiError.of(ErrorCode.UNAUTHORIZED,
                                            "authentication required", null)));
                        })
                        .accessDeniedHandler((request, response, accessDeniedException) -> {
                            response.setStatus(403);
                            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                            response.setCharacterEncoding(StandardCharsets.UTF_8.name());
                            response.getWriter().write(objectMapper.writeValueAsString(
                                    ApiError.of(ErrorCode.FORBIDDEN,
                                            "requires role " + ROLE_ADMIN, null)));
                        }));
        return http.build();
    }

    @Bean
    public UserDetailsService userDetailsService(PasswordEncoder passwordEncoder) {
        return new InMemoryUserDetailsManager(
                User.withUsername("admin")
                        .password(passwordEncoder.encode("admin123"))
                        .roles(ROLE_ADMIN)
                        .build(),
                User.withUsername("viewer")
                        .password(passwordEncoder.encode("viewer123"))
                        .roles(ROLE_VIEWER)
                        .build());
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }
}
