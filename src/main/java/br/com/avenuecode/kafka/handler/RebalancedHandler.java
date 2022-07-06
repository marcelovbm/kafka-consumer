package br.com.avenuecode.kafka.handler;

import br.com.avenuecode.kafka.entity.TruckCoordinates;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

public class RebalancedHandler implements ConsumerRebalanceListener {

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets;
    private KafkaConsumer<Long, TruckCoordinates> consumer;

    public RebalancedHandler(Map<TopicPartition, OffsetAndMetadata> currentOffsets, KafkaConsumer<Long, TruckCoordinates> consumer){
        this.currentOffsets = currentOffsets;
        this.consumer = consumer;
    }
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        consumer.commitSync(currentOffsets);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) { }

    public void addOffset(String topic, Integer partition, Long offset) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset + 1);
        this.currentOffsets.put(topicPartition, offsetAndMetadata);
    }
}
