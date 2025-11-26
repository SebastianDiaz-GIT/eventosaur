package com.softcaribbean.demo.configs;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class PolizaStreamConfig {

    @Bean
    public KStream<String, String> polizaStream(StreamsBuilder builder) {

        // 1. Leemos el topic
        KStream<String, String> stream = builder.stream("polizas-test");

        // 2. Agrupamos por KEY y aplicamos agregación incremental
        KTable<String, String> tablaEstado = stream
                .groupByKey()
                .aggregate(
                        () -> "{}", // estado inicial
                        (key, evento, estadoPrevio) -> aplicarEvento(estadoPrevio, evento),
                        Materialized.as("poliza-store")   // nombre del state store
                );

        return stream;
    }

    /** Aplica el evento al estado actual */
    private String aplicarEvento(String estadoAnterior, String evento) {
        return evento; // ejemplo simple
    }
}

