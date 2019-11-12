package com.bupt.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class PartitionerProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
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

        //添加分区器
        props.put("partitioner.class","com.bupt.producer.com.bupt.partitioner.MyPartitioner");
        //9 创建生产者对象
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 10; i++) {
            //10 生产者发送数据
            producer.send(new ProducerRecord<String, String>("first", "hello " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        System.out.println(recordMetadata.partition()+"----"+recordMetadata.offset());
                    }
                    else{
                        e.printStackTrace();
                    }
                }
            });
        }
//        Thread.sleep(100);
        //11 关闭资源 会把内存资源清掉
        producer.close();

    }
}
