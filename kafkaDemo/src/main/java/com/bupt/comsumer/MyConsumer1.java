package com.bupt.comsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer1 {

    public static void main(String[] args) {
        //1 创建消费者配置信息
        Properties properties = new Properties();
        //2 给配置信息复制
        //连接集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop101:9092");
        //消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test1");
        //开启自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //自动提交的延时
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //key value的反序列化
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //3 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //4 订阅主题
        consumer.subscribe(Arrays.asList("first","second"));

        while(true){
            //获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            //解析并打印
            for(ConsumerRecord<String, String> Records:consumerRecords){
                System.out.println(Records.offset()+"--------"+Records.key()+"------"+Records.value());
            }
            // consumer.commitSync();
        }

    }

}
