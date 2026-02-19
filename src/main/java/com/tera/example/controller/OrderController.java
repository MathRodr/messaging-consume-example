package com.tera.example.controller;

import com.tera.example.model.Order;
import com.tera.example.publish.KafkaExamplePublish;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    @Autowired
    private KafkaExamplePublish kafkaExamplePublish;

    @PostMapping
    public ResponseEntity<String> createOrder() {

        Order order = new Order(
                UUID.randomUUID().toString(),
                "user123",
                "CREATED"
        );

        kafkaExamplePublish.publishOrderCreated(order);

        return ResponseEntity.accepted()
                .body("Order created: " + order.orderId());
    }
}
