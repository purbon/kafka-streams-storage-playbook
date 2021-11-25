package com.purbon.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Properties;

public class App {

    public static Properties config() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        config.put(StreamsConfig.CLIENT_ID_CONFIG, "client");

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);

        return config;
    }

    public static void main(String[] args) throws Exception {

        StreamsBuilder builder = new StreamsBuilder();

        var table = builder.table("oof", Materialized.with(Serdes.String(), Serdes.String()));

                builder.stream("foo", Consumed.with(Serdes.String(), Serdes.String()))
                        .join(table, (s, s2) -> s+"#"+s2)
                        .peek((k, v) -> System.out.println(k+" - "+v))
                        .to("bar");

        KafkaStreams streams = new KafkaStreams(builder.build(), config());

        streams.setUncaughtExceptionHandler((thread, ex) -> System.out.println(ex));
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp();
        streams.start();

    }
}
