package com.github.lisama569.reliableoutboxdeliveryjava.service;

public class ConflictException extends RuntimeException {

    public ConflictException(String message) {
        super(message);
    }
}
