package com.softcaribbean.demo.controllers;

import com.softcaribbean.demo.services.dtos.PolizaEvent;
import com.softcaribbean.demo.services.interfaces.KafkaEventGateway;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.MediaType;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@Log4j2
@RequestMapping(value = "/api/kafka-consumer", produces = MediaType.APPLICATION_JSON_VALUE)
@RequiredArgsConstructor
public class KafkaConsumerService {
    private final KafkaEventGateway<PolizaEvent> kafkaEventGateway;
    private final StreamsBuilderFactoryBean factoryStreams;

    @PostMapping("/create-event")
    public Mono<PolizaEvent> createEvent(@RequestBody PolizaEvent event) {
        return kafkaEventGateway.sendEvent(event);
    }

    @GetMapping("/status-poliza/{idPoliza}")
    public Mono<String> getStatusPoliza(@PathVariable String idPoliza) {
        ReadOnlyKeyValueStore<String, String> store= factoryStreams.getKafkaStreams().store(StoreQueryParameters.fromNameAndType("poliza-store", QueryableStoreTypes.keyValueStore()));
        return Mono.fromCallable( () -> store.get(idPoliza));

    }


}
