package com.mobin.Kafka.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.janino.IClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/8/14
 * Time: 3:40 PM
 * 每处理完10消息提交一次
 */
public class QuotationConsumerManualCommit {
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
        properties.put("fetch.max.bytes", 1024);  //设置一次fetch请求取得的数据最大值为1kb，默认为5MB，这里是为了方便测试
        properties.put("enable.auto.commit", false);  //手动提交偏移量
        properties.put("client.id", CLIENT_ID);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    public static void poll(){
        consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
               long committedOffset = -1;
               for (TopicPartition topicPartition: partitions){
                   committedOffset = consumer.committed(topicPartition).offset();
                   System.out.println("当前"+topicPartition+"偏移量："+committedOffset);
                   consumer.seekToBeginning(partitions);
               }
            }
        });
        try {
            int minCommitSize = 10;//最少处理10条消息后才进行提交
            int count = 0; //消息计算器
          while (true){
             ConsumerRecords<String, String> records = consumer.poll(1000);
             for (ConsumerRecord<String, String> record: records){
                 System.out.printf("partition = %d, offset = %d, key = %s value = %s%n",
                         record.partition(), record.offset(), record.key(), record.value());
                 count ++;
             }
             if (count >= minCommitSize) {
                 consumer.commitAsync(new OffsetCommitCallback() {
                     @Override
                     public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                         if (null == e){
                             System.out.println("提交成功");
                         }else {
                             System.out.println("提交发生了异常");
                         }
                     }
                 });
                 count = 0;
             }
          }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        QuotationConsumerManualCommit.poll();
    }
}
