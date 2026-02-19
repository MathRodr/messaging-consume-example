package com.tera.example.consume;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tera.example.model.Order;
import com.tera.messaging.consumer.MessageListener;
import com.tera.messaging.model.StandardMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaExampleConsumer {

    private final ObjectMapper objectMapper;

    @MessageListener(
            topic = "orders-create",
            groupId = "order-service-example",
            eventType = "order.created",
            concurrency = 3,
            autoAck = true
    )
    public void handleOrderCreated(StandardMessage<?> message) {
        log.info("Recebida ordem criada: messageId={}, correlationId={}",
                message.getMessageId(), message.getCorrelationId());

        // Converter payload para o tipo específico
        Order order = objectMapper.convertValue(message.getPayload(), Order.class);

        log.info("Processando ordem: orderId={}, userId={}, status={}",
                order.orderId(), order.userId(), order.status());

        // Processar a ordem...
    }

    // Exemplo 2: Consumer com controle manual de acknowledge
    @MessageListener(
            topic = "orders-create",
            groupId = "payment-processor-group",
            eventType = "order.created",
            autoAck = false  // Controle manual do ack
    )
    public void processPayment(StandardMessage<?> message, Acknowledgment ack) {
        try {
            Order order = objectMapper.convertValue(message.getPayload(), Order.class);

            log.info("Processando pagamento para ordem: {}", order.orderId());

            // Processar pagamento...
            boolean paymentSuccessful = processPaymentLogic(order);

            if (paymentSuccessful) {
                // Sucesso - acknowled da mensagem
                ack.acknowledge();
                log.info("Pagamento processado com sucesso: {}", order.orderId());
            } else {
                // Falha - não faz ack, mensagem será reprocessada
                log.warn("Falha no processamento do pagamento: {}", order.orderId());
            }

        } catch (Exception e) {
            log.error("Erro ao processar pagamento: {}", e.getMessage(), e);
            // Não faz ack em caso de erro
        }
    }

    // Exemplo 3: Consumer que escuta todos os eventos (sem filtro de eventType)
    @MessageListener(
            topic = "audit-topic",
            groupId = "audit-group"
    )
    public void auditAllEvents(StandardMessage<?> message) {
        log.info("Auditando evento: type={}, source={}, timestamp={}",
                message.getEventType(),
                message.getSource(),
                message.getTimestamp());

        // Salvar no banco de auditoria...
    }

    private boolean processPaymentLogic(Order order) {
        return true;
    }

}
