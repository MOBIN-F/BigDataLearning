package com.mobin.Kafka;


import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2018/8/12
 * Time: 5:18 PM
 */
public class Topic {
    private static final String ZK_CONNECT = "localhost:2181";
    //ZK连接session过期时间
    private static final int SESSION_TIMEOUT = 30000;
    //连接超时时间
    private static final int CONNECT_TIMEOUT = 30000;

    public static void createTopic(AdminClient adminClient,String topic, int partition, short replica, Properties conf){

        Map<String, String> configs = new HashMap<>();
        try {
            CreateTopicsResult result = adminClient.createTopics(Arrays.asList(new NewTopic(topic, partition, replica).configs(configs)));
        }catch (Exception e){

        }finally {
            adminClient.close();
        }
    }

    public static void deleteTopic(AdminClient adminClient,String topic, Properties conf){
        adminClient.create(conf);
        KafkaFuture future = adminClient.deleteTopics(Arrays.asList(topic)).all();
        try {
            future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void updateTopicConfig(AdminClient adminClient, String topic) throws ExecutionException, InterruptedException {
        Config config = new Config(Arrays.asList(new ConfigEntry("max.message.bytes","404800")));
        adminClient.alterConfigs(Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, topic), config)).all().get();
    }

    public static void showTopic(AdminClient adminClient, String topic) throws ExecutionException, InterruptedException {
        DescribeTopicsResult topicsResult = adminClient.describeTopics(Arrays.asList(topic));
        Map<String, TopicDescription> map = topicsResult.all().get();
        for (Map.Entry<String, TopicDescription> entry: map.entrySet()){
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }

    }

    //查询所有Topics
    public static void showAllTopic(AdminClient adminClient) throws ExecutionException, InterruptedException {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult result = adminClient.listTopics(options);
        Set<String> topicName = result.names().get();
        System.out.println(topicName);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String TOPIC = "APITopic";
        Properties conf = new Properties();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(conf);
//        Topic.createTopic(adminClient,"APITopic",1, (short) 1, conf);
//        Topic.deleteTopic(adminClient, TOPIC ,conf);
//        Topic.updateTopicConfig(adminClient, TOPIC);
//        Topic.showTopic(adminClient, TOPIC);
        Topic.showAllTopic(adminClient);
    }
}
