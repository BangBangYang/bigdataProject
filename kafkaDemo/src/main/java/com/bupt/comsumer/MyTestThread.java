package com.bupt.comsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
public class MyTestThread {

    public static void main(String[] args) {
        new Thread(new MyConsumerThread(),"a").start();

        new Thread(new MyConsumerThread(),"b").start();

       // new Thread(new MyConsumerThread(),"c").start();
    }
}

class MyConsumerThread implements Runnable {

    KafkaConsumer<String, String> consumer;

    public  MyConsumerThread(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop101:9092");
        //消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        //开启自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //自动提交的延时
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //key value的反序列化
        //properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //3 创建消费者
        consumer = new KafkaConsumer<String, String>(properties);
    }
    @Override
    public void run() {
        consumer.subscribe(Arrays.asList("first"));
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(Thread.currentThread().getName()+"进行了消费"+record.value()+"分区是"+record.partition()+"偏移量是"+record.offset());
            }

        }

    }
}
