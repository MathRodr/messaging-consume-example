package com.tera.example.model;

public record Order(
        String orderId,
        String userId,
        String status
) {}
