package br.com.avenuecode.kafka.callback;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class CustomOffsetCommitCallback implements OffsetCommitCallback {

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
        if (e != null)
            e.printStackTrace();
    }
}
