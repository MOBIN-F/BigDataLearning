package com.mobin.Kafka.Producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.util.resources.ga.LocaleNames_ga;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/8/13
 * Time: 3:08 PM
 */
public class KafkaProducerThread implements Runnable {
    private static final int MSG_SIZE = 100;
    private static final String TOPIC = "stock-quotation4";
    private static final String BROKER_LIST = "localhost:9092";
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerThread.class);
    private static KafkaProducer<String, String> producer = null;
    private ProducerRecord<String, String> record = null;

    public KafkaProducerThread(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        this.producer = producer;
        this.record = record;
    }

    @Override
    public void run() {
        System.out.println(producer + record.toString());
        producer.send(record, new Callback() {

            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println("00000");
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (null != e) {  //发送消息异常
                            log.error("发送消息异常...");
                        }
                        if (null != recordMetadata) {
                            log.info(String.format("offset:%s, partition:%s", recordMetadata.offset(), recordMetadata.partition()));
                        }
                    }
                });
            }
        });
    }

    private static StockQuotationInfo createQuotationInfo() {
        StockQuotationInfo quotationInfo = new StockQuotationInfo();
        Random random = new Random();
        Integer stockCode = 600100 + random.nextInt();
        float r = (float) Math.random();
        if (r / 2 < 0.5) {
            r = -r;
        }
        DecimalFormat decimalFormat = new DecimalFormat(".00");
        quotationInfo.setCurrentPrice(Float.valueOf(decimalFormat.format(11 + r)));
        quotationInfo.setPreClosePrice(11.80f);
        quotationInfo.setOpenPrice(11.5f);
        quotationInfo.setLowPrice(10.5f);
        quotationInfo.setHighPrice(12.5f);
        quotationInfo.setStockCode(stockCode.toString());
        quotationInfo.setTradeTime(System.currentTimeMillis());
        quotationInfo.setStockName("股票-" + stockCode);
        return quotationInfo;
    }

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        Properties configs = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);
        ProducerRecord<String, String> record;
        StockQuotationInfo quotationInfo;
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        long current = System.currentTimeMillis();
        try {
            for (int i = 0; i < MSG_SIZE; i++) {
                quotationInfo = createQuotationInfo();
                record = new ProducerRecord<String, String>(TOPIC, null, quotationInfo.getTradeTime(),
                        quotationInfo.getStockCode(), quotationInfo.toString());
                executorService.submit(new KafkaProducerThread(producer, record));
            }
        } catch (Exception e) {
            System.out.println("-------");
        } finally {
            producer.close();
            executorService.shutdown();
        }
    }
}
