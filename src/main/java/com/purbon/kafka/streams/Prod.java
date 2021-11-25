package com.purbon.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class Prod {

    public static Properties config() {
        final Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());

        return config;
    }

    public static void main(String [] args) {

        KafkaProducer<String, String> prod = new KafkaProducer<>(config());
        int i=0;
        while(true) {
            var k = "key:"+ i;
            var v = "value:"+ i;
            var record = new ProducerRecord<>("oof", k, v);
            prod.send(record);
            i++;
            if (i > 10) {
                System.out.println("flush");
                prod.flush();
            }
        }

    }
}
