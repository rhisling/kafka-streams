package me.aravindh;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

public class App {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-starter-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textlines = builder.stream("fav-color-input");

        KStream<String, String> userAndColors = textlines.filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((user, color) -> new HashSet<>(Arrays.asList("red", "green", "blue")).contains(color));

        userAndColors.to("user-keys-and-colors");

        KTable<String, String> userAndColorsTable = builder.table("user-keys-and-colors");

        KTable<String, Long> favColors = userAndColorsTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count("CountsByColors");


        favColors.to(Serdes.String(), Serdes.Long(), "fav-color-output");

        KafkaStreams streams = new KafkaStreams(builder, properties);
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }
}
