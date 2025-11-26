package com.softcaribbean.demo.controllers;

import com.softcaribbean.demo.services.dtos.PocResponse;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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

    @GetMapping("/generar/{cantidad}")
    public Mono<PocResponse> generarEventosMasivos(@PathVariable Integer cantidad) {

        long start = System.currentTimeMillis();

        // Lista para simular pólizas ya existentes (se llena conforme el sistema crea nuevas)
        List<String> polizasCreadas = new ArrayList<>();

        return Flux.range(0, cantidad)
                .map(i -> {

                    PolizaEvent event = new PolizaEvent();

                    // Regla:
                    // 80% nuevos - 20% actualizaciones
                    boolean esActualizacion = !polizasCreadas.isEmpty() && Math.random() < 0.20;

                    if (esActualizacion) {
                        // Tomamos una existente al azar
                        String polizaExistente = polizasCreadas.get(
                                (int) (Math.random() * polizasCreadas.size())
                        );

                        event.setIdPoliza(polizaExistente);
                        event.setExtraData("ACTUALIZACION de póliza: " + polizaExistente
                                + " => " + UUID.randomUUID());
                    } else {
                        // Creamos nueva póliza
                        String nueva = "POLIZA-" + UUID.randomUUID();
                        polizasCreadas.add(nueva);

                        event.setIdPoliza(nueva);
                        event.setExtraData("CREACION de póliza: " + nueva
                                + " => " + UUID.randomUUID());
                    }

                    return event;
                })
                .flatMap(kafkaEventGateway::sendEvent, 300) // 300 en paralelo
                .collectList()
                .map(signal -> {
                    long end = System.currentTimeMillis();
                    System.out.println("==== PRUEBA FINALIZADA ====");
                    System.out.println("Eventos generados: " + cantidad);
                    System.out.println("Pólizas creadas: " + polizasCreadas.size());
                    System.out.println("Tiempo total: " + (end - start) + " ms");
                    System.out.println("Velocidad: " +
                            (cantidad * 1000.0 / (end - start)) + " eventos/s");
                    PocResponse response = new PocResponse();
                    response.setPocStatus("==== PRUEBA FINALIZADA ====\n" +
                            "Eventos generados: " + cantidad + "\n" +
                            "Pólizas creadas: " + polizasCreadas.size() + "\n" +
                            "Tiempo total: " + (end - start) + " ms\n" +
                            "Velocidad: " + (cantidad * 1000.0 / (end - start)) + " eventos/s");
                    return response;
                });
    }


    @GetMapping("/status-poliza/{idPoliza}")
    public Mono<String> getStatusPoliza(@PathVariable String idPoliza) {
        ReadOnlyKeyValueStore<String, String> store= factoryStreams.getKafkaStreams().store(StoreQueryParameters.fromNameAndType("poliza-store", QueryableStoreTypes.keyValueStore()));
        return Mono.fromCallable( () -> store.get(idPoliza));

    }


}
