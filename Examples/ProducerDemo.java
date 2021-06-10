/*
Simple Kafka producer example
Zookeeper and kafka server have to be started at first
Please change the bootstrapServers and topic as needed
*/

package com.github.marmuedev.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        //insert your Server IP and port as string value
        String bootstrapServers = "127.0.0.1:9092";

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record - please insert your kafka topic name instead of "first-topic"
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "Hello from Java");

        //send data to kafka
        producer.send(record);
        //force to send data
        producer.flush();
        //use this instead of flush at the end of the send process to flush and end process
        producer.close();
    }

}
