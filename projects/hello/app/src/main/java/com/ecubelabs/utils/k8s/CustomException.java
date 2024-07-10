package com.ecubelabs.utils.k8s;

public class CustomException extends RuntimeException {
    public CustomException(String message) {
        super(message);
    }
}