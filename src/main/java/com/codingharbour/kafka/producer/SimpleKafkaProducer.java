package com.codingharbour.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleKafkaProducer {

    public static void main(String[] args) {
        //create kafka producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "35.226.135.93:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> producer = new KafkaProducer<>(properties);

        //prepare the record
        String recordValue = "Current time is " + Instant.now().toString();
        System.out.println("Sending message: " + recordValue);
        ProducerRecord<String, String> record = new ProducerRecord<>("test", null, recordValue);

        //produce the record
        try {
            Future<RecordMetadata> send = producer.send(record);
            RecordMetadata recordMetadata = send.get();
            System.out.println("recordMetadata:" + recordMetadata);
            producer.flush();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        //close the producer at the end
        producer.close();
    }
}
