package br.com.avenuecode.kafka;

import br.com.avenuecode.kafka.deserializer.CustomDeserializer;
import br.com.avenuecode.kafka.entity.TruckCoordinates;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MainConsumer {

    private static final String TOPIC_NAME = "avenuecode-partitions";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "avenueCodeGroup");

        try(KafkaConsumer<Long, TruckCoordinates> consumer = new KafkaConsumer<>(properties)){
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            ConsumerRecords<Long, TruckCoordinates> consumerRecords = consumer.poll(Duration.ofSeconds(20));
            for (ConsumerRecord<Long, TruckCoordinates> consumerRecord: consumerRecords) {
                System.out.println("Key: " + consumerRecord.key() + " Value: " + consumerRecord.value());
                System.out.println("Partition: " + consumerRecord.partition());
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
