package com.mobin.Kafka.consumers;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/8/16
 * Time: 3:50 PM
 */
public class KafkaConsumerThread extends Thread{
    //每个线程拥有私有的KafkaConsumer实例
    private KafkaConsumer<String,String> consumer;

    @Override
    public void run() {
        super.run();
    }
}
