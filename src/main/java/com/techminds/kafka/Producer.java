package com.techminds.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    public static final String topicName="topic-2";

    public static void main(String args[]){

        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<String, String>(properties);

        try {
            ProducerRecord<String,String> producerRecord=new ProducerRecord<String, String>(topicName,"","Hello Nishant");
            kafkaProducer.send(producerRecord);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}
