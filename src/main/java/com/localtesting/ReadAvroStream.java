package com.localtesting;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ReadAvroStream {
    public static void main(String[] args) {
        // Generic Avro serde example
// When configuring the default serdes of StreamConfig
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-1");
        streamsConfiguration.put("schema.registry.url", "http://localhost:8081");

// When you want to override serdes explicitly/selectively
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
                "http://localhost:8081");
        final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
        keyGenericAvroSerde.configure(serdeConfig, true); // `true` for record keys
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false); // `false` for record values

        StreamsBuilder builder = new StreamsBuilder();
        KStream<GenericRecord, GenericRecord> textLines =
                builder.stream("avro-topic", Consumed.with(keyGenericAvroSerde, valueGenericAvroSerde));

        textLines.foreach(
                (key, value) -> {
                    System.out.println(key.get("user_id")+" "+value.get("my_field1"));
                }
        );

        KafkaStreams stream = new KafkaStreams(builder.build(), streamsConfiguration);
        stream.start();

    }
}
