package com.baron.kafka;

import com.sun.org.apache.xerces.internal.dom.PSVIAttrNSImpl;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/***
 @package com.baron.kafka
 @author Baron
 @create 2020-09-12-6:11 PM
 */
public class MyConsumer {

    private static KafkaConsumer<String, String> consumer;
    private static Properties properties;

    static {
        properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id" , "KafkaStudy");

    }

    private static void generalConsumeMessageAutoCommit() {

        properties.put("enable.auto.commit", true);
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("kafka-study-x"));

        try {
            while (true) {
                boolean flag = true;
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic = %s, partition = %s, key = %s, value = %s", record.topic(), record.partition(), record.key(), record.value()));
                    if (record.value().equals("done")) {
                        flag = false;
                    }
                }

                if (!flag) {
                    break;
                }
            }
        }finally {
            consumer.close();
        }
    }

    public static void generalConsumeMessageSyncCommit() {

        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("kafka-study-y"));
        while (true) {
            boolean flag  = true;

            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("topic = %s, partition = %s, key = %s, value = %s", record.topic(), record.partition(), record.key(), record.value()));
                if (record.value().equals("done")) {
                    flag = false;
                }
            }

            try {
                //线程会阻塞
                consumer.commitSync();
            } catch (CommitFailedException ex) {
                System.out.println("commit failed error: {}" + ex.getMessage());
            }

            if (!flag) {
                break;
            }
        }


    }

    public static void generalConsumeMessageAsyncCommit() {

        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("kafka-study-y"));
        while (true) {
            boolean flag  = true;

            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("topic = %s, partition = %s, key = %s, value = %s", record.topic(), record.partition(), record.key(), record.value()));
                if (record.value().equals("done")) {
                    flag = false;
                }
            }

            // commit A, offset 2000
            // commit B, offset 3000

                //线程不会阻塞 失败不重试
                consumer.commitAsync();

            if (!flag) {
                break;
            }
        }

    }

    private static void generalConsumeMessageAsyncCommitWithCallback() {

        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("kafka-study-y"));
        while (true) {
            boolean flag  = true;

            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("topic = %s, partition = %s, key = %s, value = %s", record.topic(), record.partition(), record.key(), record.value()));
                if (record.value().equals("done")) {
                    flag = false;
                }
            }

            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if (e != null) {
                        System.out.println("commit failed for offsets: {}" + map + e.getMessage());
                    }
                }
            });

            if (!flag) {
                break;
            }
        }

    }

    private static void mixSyncAndAsyncCommit() {

        properties.put("auto.commit.offset", false);
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("kafka-study-y"));

        try {

            while (true) {
                boolean flag  = true;

                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic = %s, partition = %s, key = %s, value = %s", record.topic(), record.partition(), record.key(), record.value()));
                    if (record.value().equals("done")) {
                        flag = false;
                    }
                }

                consumer.commitAsync();
                if (!flag) {
                    break;
                }
            }

        } catch (Exception e) {
            System.out.println("commit async error: " + e.getMessage());
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {
        generalConsumeMessageAutoCommit();
    }
}

