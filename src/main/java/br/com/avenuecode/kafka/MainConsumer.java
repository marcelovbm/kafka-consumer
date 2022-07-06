package br.com.avenuecode.kafka;

import br.com.avenuecode.kafka.callback.CustomOffsetCommitCallback;
import br.com.avenuecode.kafka.deserializer.CustomDeserializer;
import br.com.avenuecode.kafka.entity.TruckCoordinates;
import br.com.avenuecode.kafka.handler.RebalancedHandler;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MainConsumer {

    private static final String TOPIC_NAME = "avenuecode-partitions";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "avenueCodeGroup");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "102412323");
        properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "200");
        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000");
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "2MB");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "TruckCoordinatesConsumer");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        try(KafkaConsumer<Long, TruckCoordinates> consumer = new KafkaConsumer<>(properties)){
            Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
            RebalancedHandler rebalancedHandler = new RebalancedHandler(currentOffsets, consumer);
            consumer.subscribe(Collections.singletonList(TOPIC_NAME), rebalancedHandler);
            while (true) {
                ConsumerRecords<Long, TruckCoordinates> consumerRecords = consumer.poll(Duration.ofSeconds(20));
                int count = 0;
                for (ConsumerRecord<Long, TruckCoordinates> consumerRecord : consumerRecords) {
                    System.out.println("Key: " + consumerRecord.key() + " Value: " + consumerRecord.value());
                    System.out.println("Partition: " + consumerRecord.partition());

                    rebalancedHandler.addOffset(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
                    if(count % 10 == 0){
                        TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(consumerRecord.offset() + 1);
                        consumer.commitAsync(Collections.singletonMap(topicPartition,offsetAndMetadata) ,new CustomOffsetCommitCallback());
                    }
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
