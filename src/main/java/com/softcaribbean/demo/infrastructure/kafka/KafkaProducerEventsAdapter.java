package com.softcaribbean.demo.infrastructure.kafka;

import com.softcaribbean.demo.services.interfaces.KafkaEventGateway;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderResult;

@Log4j2
@Component
public class KafkaProducerEventsAdapter<T> implements KafkaEventGateway<T> {
    private final ReactiveKafkaProducerTemplate<String, String> producer;
    private final ObjectMapper objectMapper;

    public KafkaProducerEventsAdapter(ObjectMapper objectMapper, ReactiveKafkaProducerTemplate<String, String> producer) {
        this.producer = producer;
        this.objectMapper = objectMapper;
        this.objectMapper.findAndRegisterModules();
    }

    @Value("${kafka.topics.data-topic}")
    private String topic;

    @Override
    public Mono<T> sendEvent(T event) {
        try {
            String message = objectMapper.writeValueAsString(event);
            String key = extractKeyFromEvent(event);
            return sendMessage(message, key).thenReturn(event);
        } catch (Exception e) {
            log.error("Error serializing object for Kafka", e);
            return Mono.error(e);
        }
    }

    /**
     * Extrae la key del evento para Kafka. Si el evento es PolizaEvent, retorna su id como String.
     * Si no tiene id o no es PolizaEvent, retorna null.
     */
    private String extractKeyFromEvent(T event) {
        if (event instanceof com.softcaribbean.demo.services.dtos.PolizaEvent polizaEvent) {
            return polizaEvent.getIdPoliza() != null ? polizaEvent.getIdPoliza().toString() : null;
        }
        // Puedes agregar más instanceof para otros tipos de eventos aquí
        return null;
    }

    private Mono<SenderResult<Void>> sendMessage(String message, String key) {
        return producer.send(topic, key, message)
                .doOnSuccess(result -> log.info("Message sent successfully to topic {}: {}", topic, message))
                .doOnError(error -> log.error("Failed to send message to topic {}: {}", topic, error.getMessage()));
    }


}
