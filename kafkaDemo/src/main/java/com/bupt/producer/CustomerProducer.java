package com.bupt.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CustomerProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String > producer=new KafkaProducer<String, String>(props);
        producer.send(new ProducerRecord<String, String>("first","1","hihi"));
        producer.close();
    }


}
