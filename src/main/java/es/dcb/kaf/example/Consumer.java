package es.dcb.kaf.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        props.put("group.id", "true");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        try {
            consumer.subscribe(Collections.singletonList("test-topic"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.print("Topic :" + record.topic());
                    System.out.print("Partition :" + record.partition());
                    System.out.print("Key :" + record.key());
                    System.out.println("value :" + record.value());
                }
            }
        }
    }
}
