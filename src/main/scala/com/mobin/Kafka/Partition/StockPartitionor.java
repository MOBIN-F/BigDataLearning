package com.mobin.Kafka.Partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/8/16
 * Time: 4:47 PM
 * //写好自定义分区后在配置文件进行自定义分区配置
 * properties.put("ProducerConfig.PARTITIONER_CLASS_CONFIG", StockPartitionor.class.getName)
 */
public class StockPartitionor implements Partitioner{
    //分区数
    private static final Integer PARTITIONS = 6;
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (key == null){
            return 0;
        }
        String stockcode = String.valueOf(key);
        try {
            int partitionID = Integer.valueOf(stockcode.substring(stockcode.length() - 2)) % PARTITIONS;
            return partitionID;
        }catch (NumberFormatException e){
             return 0;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
