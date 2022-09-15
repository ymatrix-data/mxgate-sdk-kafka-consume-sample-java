package org.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.consume.KafkaMsgConsumeCallback;
import org.example.logger.MxLogger;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerWrapper {
    private static final String TAG = KafkaConsumerWrapper.class.getName();
    private final Logger l = MxLogger.init(KafkaConsumerWrapper.class);
    private final String topic;
    private final KafkaConsumer<String, String> consumer;

    private long latestOffset;

    private int consumerNum;

    public KafkaConsumerWrapper(String bootStrapServers, String topic, int part, String groupID) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootStrapServers);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("group.id", groupID);
        props.put("session.timeout.ms", "30000");
        props.put("max.poll.records", 1000);
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.topic = topic;
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        this.consumerNum = part;
    }

    public void consumeLoop(long timeoutMillis, KafkaMsgConsumeCallback callback) {
        final int giveUp = 100;
        int noRecordsCount = 0;

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(timeoutMillis));
            if (consumerRecords.count() == 0 && consumerRecords.isEmpty()) {
                l.debug("{} pull 0 records from Kafka with latest offset = {} [{}]", TAG, this.latestOffset, this.consumerNum);
            } else {
                l.debug("{} pull {} records from Kafka with latest offset = {} [{}]", TAG, consumerRecords.count(), this.latestOffset, this.consumerNum);
            }

            consumerRecords.forEach(new java.util.function.Consumer<ConsumerRecord<String, String>>() {
                @Override
                public void accept(ConsumerRecord<String, String> stringStringConsumerRecord) {
                    String value = stringStringConsumerRecord.value();
                    KafkaConsumerWrapper.this.latestOffset = stringStringConsumerRecord.offset();
                    l.debug("Consume msg from Topic={} Partition={} Offset={}", stringStringConsumerRecord.topic(), stringStringConsumerRecord.partition(), stringStringConsumerRecord.offset());
                    callback.convertMsgValueToObj(value, KafkaConsumerWrapper.this.consumerNum);
                }
            });
        }
    }
}
