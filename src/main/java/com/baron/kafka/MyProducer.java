package com.baron.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/***
 @package PACKAGE_NAME
 @author Baron
 @create 2020-09-12-4:16 PM
 */
public class MyProducer {
    private static KafkaProducer<String, String> producer;

    static {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("partitioner.class",
                "com.baron.kafka.CustomPartitioner");

        producer = new KafkaProducer<>(properties);
    }

    private static void sendMessageForgetResult() {

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "kafka-study", "name", "ForgetResult"
        );
        producer.send(record);
        producer.close();
    }

    private static void sendMessageSync() throws Exception {

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "kafka-study", "name", "sync"
        );
        RecordMetadata result = producer.send(record).get();

        System.out.println(result.topic());
        System.out.println(result.partition());
        System.out.println(result.offset());

        producer.close();
    }

    private static void sendMessageCallback() {

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "kafka-study-x", "name", "callback"
        );
        producer.send(record, new MyProducerCallback());

        record = new ProducerRecord<>(
                "kafka-study-x", "name-x", "callback"
        );
        producer.send(record, new MyProducerCallback());

        record = new ProducerRecord<>(
                "kafka-study-x", "name-y", "callback"
        );
        producer.send(record, new MyProducerCallback());

        record = new ProducerRecord<>(
                "kafka-study-x", "name-z", "callback"
        );
        producer.send(record, new MyProducerCallback());

        producer.close();
    }

    private static class MyProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {

            if (e != null) {
                e.printStackTrace();
                return;
            }

            System.out.println(recordMetadata.topic());
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.offset());
            System.out.println("Coming in MyProducerCallback");
        }
    }

    public static void main(String[] args) throws Exception {

//        sendMessageForgetResult();
//        sendMessageSync();
        sendMessageCallback();
    }
}
