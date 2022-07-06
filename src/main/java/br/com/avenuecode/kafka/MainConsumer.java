package br.com.avenuecode.kafka;

import br.com.avenuecode.kafka.deserializer.CustomDeserializer;
import br.com.avenuecode.kafka.entity.TruckCoordinates;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MainConsumer {

    private static final String TOPIC_NAME = "avenuecode-kafka-custom";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", LongDeserializer.class.getName());
        properties.setProperty("value.deserializer", CustomDeserializer.class.getName());
        properties.setProperty("group.id", "avenueCodeGroup");

        try(KafkaConsumer<Long, TruckCoordinates> consumer = new KafkaConsumer<>(properties)){
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            ConsumerRecords<Long, TruckCoordinates> consumerRecords = consumer.poll(Duration.ofSeconds(20));
            for (ConsumerRecord<Long, TruckCoordinates> consumerRecord: consumerRecords) {
                System.out.println("Key: " + consumerRecord.key() + " Value: " + consumerRecord.value());
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
