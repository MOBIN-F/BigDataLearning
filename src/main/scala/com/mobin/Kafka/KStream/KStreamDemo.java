package com.mobin.Kafka.KStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/8/19
 * Time: 3:41 PM
 */
public class KStreamDemo {
    private static final String APPLICATION_ID_CONFIG = "KStream-test";
    private static final String BROKER_LIST = "localhost:9092";
    private static final String TOPIC = "streams-foo";
    private static StreamsBuilder streamsBuilder;
    private static KStream<String, String> textLine;

    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_CONFIG);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    public static void printf() throws InterruptedException {
        Properties properties = initProperties();
        streamsBuilder = new StreamsBuilder();
        textLine = streamsBuilder.stream(TOPIC);
        textLine.foreach(new ForeachAction<String, String>() {
            @Override
            public void apply(String key, String value) {
                System.out.println(key + ":" + value);
            }
        });
        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
        Thread.sleep(5000L);
        streams.close();
    }

    public static void main(String[] args) throws InterruptedException {
        KStreamDemo.printf();
    }

}
