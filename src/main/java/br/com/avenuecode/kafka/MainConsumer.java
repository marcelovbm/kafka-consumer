package br.com.avenuecode.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MainConsumer {

    private static final String TOPIC_NAME = "avenuecode-kafka";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", LongDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "avenueCodeGroup");

        try(KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(properties)){
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(20));
            for (ConsumerRecord<Long, String> consumerRecord: consumerRecords) {
                System.out.println("Key: " + consumerRecord.key() + " Value: " + consumerRecord.value());
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
