package com.softcaribbean.demo.services.interfaces;

import reactor.core.publisher.Mono;

public interface KafkaEventGateway<T> {
    Mono<T> sendEvent(T event);
}
