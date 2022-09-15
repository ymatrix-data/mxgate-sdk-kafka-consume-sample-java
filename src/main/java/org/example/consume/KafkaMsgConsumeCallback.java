package org.example.consume;

/**
 * Kafka consumer 的回调函数。
 */
public interface KafkaMsgConsumeCallback {
    void convertMsgValueToObj(String value, int consumerNum);
}
