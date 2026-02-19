package com.tera.example.publish;

import com.tera.example.model.Order;
import com.tera.messaging.model.StandardMessage;
import com.tera.messaging.publisher.MessagePublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaExamplePublish {

    private final MessagePublisher messagePublisher;

    public void publishOrderCreated(Order order) {
        // Exemplo 1: Publicar com StandardMessage completo
        StandardMessage<Order> message = StandardMessage.<Order>builder()
                .eventType("order.created")
                .correlationId(order.orderId())
                .payload(order)
                .version("1.0")
                .build();

        message.addMetadata("userId", "user-123");
        message.addMetadata("channel", "web");

        messagePublisher.publish("orders-create", message)
                .thenAccept( (messageId) -> {
                    log.info("Mensagem publicada com ID: {}", messageId);
                }).exceptionally((ex) -> {
                    log.error("Erro ao publicar mensagem: {}", ex.getMessage());
                    return null;
                });

        // Exemplo 2: Publicar apenas o payload (mais simples)
        messagePublisher.publishPayload("orders-topic", "order.created", order)
                .thenAccept(messageId -> log.info("Ordem publicada: {}", messageId));

        // Exemplo 3: Publicar com chave de particionamento (importante para ordem)
        messagePublisher.publish("orders-topic", order.userId(), message)
                .thenAccept(messageId -> log.info("Ordem publicada com chave: {}", messageId));
    }


}
