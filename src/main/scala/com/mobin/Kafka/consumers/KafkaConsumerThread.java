package com.mobin.Kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/8/16
 * Time: 3:50 PM
 * 6个消费者线程消费同一个主题
 */
public class KafkaConsumerThread extends Thread {
    //每个线程拥有私有的KafkaConsumer实例
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerThread(Properties consumerConfig, String topic) {
        this.consumer = new KafkaConsumer<String, String>(consumerConfig);
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %d, offset = %d, key = %s value = %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", true);
        properties.put("auto.commit.interval.ms", 1000);//设置偏移量提交时间
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        for (int i = 0; i < 6; i ++){
            new KafkaConsumerThread(properties, "stock-quotation").start();
        }
    }
}
