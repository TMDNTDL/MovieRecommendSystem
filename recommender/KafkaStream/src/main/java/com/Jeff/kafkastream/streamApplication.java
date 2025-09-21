package com.Jeff.kafkastream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class streamApplication {
    public static void main(String[] args) {
        String broker = "localhost:9092";

        String from = "log";
        String to = "recommender";

        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Config kafka with above setting
        StreamsBuilder builder = new StreamsBuilder();
        KStream<byte[], String> Upperstream = builder.stream(from);

        // Define topology of processing the data
        KStream<byte[], String> processor = Upperstream
                .filter((dummy, value) -> value != null && value.contains("MOVIE_RATING_PREFIX:"))
                .mapValues(


                        value -> {
                            System.out.println("movie rating data coming! >>>>>>>>>>>" + value);
                            return value.split("MOVIE_RATING_PREFIX:")[1].trim();
                        }
                );

        processor.to(to, Produced.with(Serdes.ByteArray(), Serdes.String()));
//        config.stream(from)
//                .processValues(() -> new LogProcessor())
//                .to(to);

        // kafkaStream
        KafkaStreams streams = new KafkaStreams(builder.build(), settings);
        streams.start();

        System.out.println("Kafka Stream Started! >>>>>>>>>>>>>>>>>");

    }
}
