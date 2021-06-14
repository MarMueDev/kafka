/*simple kafka producer callback example
Zookeeper and kafka server have to be started at first
Please change the bootstrapServers and the topic values as needed
*/
package com.github.marmuedev.kafka.test1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallbackDemo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerCallbackDemo.class);

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
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //will be executed every time record is send
                if (e == null){
                    //success
                    logger.info("Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    //fail
                    logger.error("Error!", e);
                }
            }
        });
        //force to send data
        producer.flush();
        //use this instead of flush at the end of the send process to flush and end process
        producer.close();
    }

}
