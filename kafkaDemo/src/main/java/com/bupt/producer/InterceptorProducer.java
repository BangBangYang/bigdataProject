package com.bupt.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

public class InterceptorProducer {
    public static void main(String[] args) {
        //1. 创建kafka配置信息
        Properties props = new Properties();
        //2 kafka集群，broker-list
        props.put("bootstrap.servers", "hadoop101:9092");
        //3 ack应答级别
        props.put("acks", "all");
        //4 重试次数
        props.put("retries", 1);
        //5 批次大小
        props.put("batch.size", 16384);
        //6 等待时间
        props.put("linger.ms", 1);
        //7 RecordAccumulator缓冲区大小
        props.put("buffer.memory", 33554432);
        //8 key value 序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //添加数据拦截链
        ArrayList<String> interceptors = new ArrayList<String>();
        interceptors.add("com.bupt.interceptor.timeInterceptor");
        interceptors.add("com.bupt.interceptor.countInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);
        //9 创建生产者对象
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            //10 生产者发送数据
            producer.send(new ProducerRecord<String, String>("first","hello yk "+i));
        }
//        Thread.sleep(100);
        //11 关闭资源 会把内存资源清掉
        producer.close();
    }
}
