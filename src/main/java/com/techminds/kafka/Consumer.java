package com.techminds.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class Consumer {

    public static final String topicName1="topic-1";

    public static void main(String args[]){

        Properties properties=new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", UUID.randomUUID().toString());
        //properties.put("client.id", "test");
        //properties.put("enable.auto.commit", "false");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String, String>(properties);

        List<String> topics=new ArrayList<String>();
        topics.add(topicName1);

        kafkaConsumer.subscribe(topics);

        /*while (true)*/{
            try {
                 ConsumerRecords<String,String> consumerRecords=kafkaConsumer.poll(100);

                 for (ConsumerRecord<String,String> consumerRecord:consumerRecords){
                     System.out.print("Topic - "+consumerRecord.topic());
                     System.out.printf("offset = %d, key = %s, value = %s", consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                 }
                kafkaConsumer.commitSync();
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                kafkaConsumer.close();
            }

        }



    }
}
