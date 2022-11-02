package es.dcb.kaf.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks","all");
        props.put("retries",0);
        props.put("batch.size",16384);
        props.put("buffer.memory",33554432);

        KafkaProducer<String, String> prod = new KafkaProducer<String, String>(props);
        String topic = "topic-test";
        int partition = 0;
        String key = "testKey";
        String value = "testValue";
        //try {
        //    prod.send(new ProducerRecord<>(topic,partition,key,value)).get() ;
        //} catch (InterruptedException | ExecutionException e) {
        //    e.printStackTrace();
        //}

        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        prod.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    System.out.println("Send failed for record");
                }
            }
        });

        prod.close();
    }
}