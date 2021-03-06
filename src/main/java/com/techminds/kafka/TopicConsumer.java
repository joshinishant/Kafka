package com.techminds.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class TopicConsumer {

    public static final String topicName1="topic-2";

    public static void main(String args[]){

        Properties properties=new Properties();
        properties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        //properties.put("client.id", "test");
        //properties.put("enable.auto.commit", "false");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String, String>(properties);

        List<TopicPartition> topicPartitions=new ArrayList<TopicPartition>();
        topicPartitions.add(new TopicPartition(topicName1,0));

        kafkaConsumer.assign(topicPartitions);
        try {
            while (true){

                ConsumerRecords<String,String> consumerRecords=kafkaConsumer.poll(100);

                for (ConsumerRecord<String,String> consumerRecord:consumerRecords){
                    System.out.print("Topic - "+consumerRecord.topic()+" ");
                    System.out.printf("offset = %d, key = %s, value = %s\n", consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                }
                kafkaConsumer.commitSync();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaConsumer.close();
        }



    }
}
