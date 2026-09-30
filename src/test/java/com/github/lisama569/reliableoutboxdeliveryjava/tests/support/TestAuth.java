package com.github.lisama569.reliableoutboxdeliveryjava.tests.support;

import org.springframework.http.HttpHeaders;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class TestAuth {

    private TestAuth() {
    }

    public static HttpHeaders ops() {
        return basic("ops", "ops-pass");
    }

    public static HttpHeaders viewer() {
        return basic("viewer", "viewer-pass");
    }

    public static HttpHeaders basic(String user, String pass) {
        HttpHeaders headers = new HttpHeaders();
        String token = Base64.getEncoder().encodeToString(
                (user + ":" + pass).getBytes(StandardCharsets.UTF_8));
        headers.set(HttpHeaders.AUTHORIZATION, "Basic " + token);
        return headers;
    }
}
