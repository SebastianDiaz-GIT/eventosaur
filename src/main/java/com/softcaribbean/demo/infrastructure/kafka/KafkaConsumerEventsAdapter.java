package com.softcaribbean.demo.infrastructure.kafka;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

@Service
@RequiredArgsConstructor
public class KafkaConsumerEventsAdapter {
    public KafkaConsumerEventsAdapter(ReceiverOptions<String, String> options){
        KafkaReceiver.create(options).receive()
                .doOnNext(rec -> {
                    System.out.println("Received message: " + rec.value());
                    rec.receiverOffset().acknowledge();
                })
                .subscribe();
    }
}
