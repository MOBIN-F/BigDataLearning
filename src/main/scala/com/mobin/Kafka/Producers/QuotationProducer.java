package com.mobin.Kafka.Producers;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.catalyst.expressions.Rand;
import org.apache.spark.sql.execution.columnar.NULL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/8/13
 * Time: 11:24 AM
 */
public class QuotationProducer {
    private static final Logger log = LoggerFactory.getLogger(QuotationProducer.class);
    private static final int MSG_SIZE = 100;
    private static final String TOPIC = "stock-quotation2";
    private static final String BROKER_LIST = "localhost:9092";
    private static KafkaProducer<String ,String> producer = null;
    static {
        Properties configs = initConfig();
        producer = new KafkaProducer<String, String>(configs);
    }

    public static Properties initConfig(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static StockQuotationInfo createQuotationInfo(){
        StockQuotationInfo quotationInfo = new StockQuotationInfo();
        Random random = new Random();
        Integer stockCode = 600100 + random.nextInt();
        float r = (float) Math.random();
        if (r / 2 < 0.5){
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

    public static void main(String[] args) {
        ProducerRecord<String, String> record = null;
        StockQuotationInfo quotationInfo = null;
        try {
            int num = 0;
            for (int i = 0; i < MSG_SIZE; i ++){
                quotationInfo = createQuotationInfo();
                record = new ProducerRecord<String, String>(TOPIC,null, quotationInfo.getTradeTime(),quotationInfo.getStockCode()
                ,quotationInfo.toString());
                producer.send(record);
                //异步方式，指定Callback，实现onCompleteion
//                producer.send(record, new Callback() {
//                    @Override
//                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                        if (null != e){  //发送消息异常
//                            log.error("发送消息异常...");
//                        }
//                        if (null != recordMetadata){
//                            log.info(String.format("offset:%s, partition:%s", recordMetadata.offset(), recordMetadata.partition()));
//                        }
//                    }
//                });
                if (num++ % 10 == 0){
                    Thread.sleep(2000L);
                }
            }
        }catch (InterruptedException e){

        }finally {
            producer.close();
        }
    }
}
