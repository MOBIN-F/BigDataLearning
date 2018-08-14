package com.mobin.Kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/8/14
 * Time: 3:40 PM
 */
public class QuotationConsumer {
    private static final String BROKERS_LIST = "localhost:9092";
    private static final String GROUP_ID = "test";
    private static final String CLIENT_ID = "test";
    private static final String TOPIC = "stock-quotation";
    private static KafkaConsumer<String, String> consumer;

    static {
        Properties properties = initPorerties();
        consumer = new KafkaConsumer<String, String>(properties);
    }

    public static Properties initPorerties(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BROKERS_LIST);
        properties.put("group.id", GROUP_ID);
        properties.put("client.id", CLIENT_ID);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    public static void poll(){
        consumer.subscribe(Arrays.asList(TOPIC));
        try {
          while (true){
              ConsumerRecords<String, String> records = consumer.poll(1000);
              for (ConsumerRecord<String, String> record: records){
                  System.out.printf("partition = %d, offset = %d, key = %s value = %s%n",
                          record.partition(), record.offset(), record.key(), record.value());
              }
          }
        }catch (Exception e){

        }finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        QuotationConsumer.poll();
    }
}
