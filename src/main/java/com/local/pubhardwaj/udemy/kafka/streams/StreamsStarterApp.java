package com.local.pubhardwaj.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class StreamsStarterApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.GLOBAL_CONSUMER_PREFIX+"auto.offset.reset", "earliest");
        properties.put(StreamsConfig.globalConsumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> wordCountInput = builder.stream("word-count-input");
        KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> value.toLowerCase(Locale.ROOT))
                                                        .flatMapValues(value -> Arrays.asList(value.split(" ")))
                                                        .selectKey((ignoredKey, word) -> word)
                                                        .groupByKey()
                                                        .count(Materialized.as("Counts"));
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
        System.out.println(streams);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

//        return builder.build();

    }
}
