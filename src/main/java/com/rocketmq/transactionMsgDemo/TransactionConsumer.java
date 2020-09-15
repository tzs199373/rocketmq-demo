package com.rocketmq.transactionMsgDemo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;

@SuppressWarnings("all")
public class TransactionConsumer {
    private static final String transactionConsumerGroup = "transactionConsumerGroup";
    private static String namesrvAddr = "127.0.0.1:9876";
    private static final String topic = "transactionTopic";

    public static void main(String[] args) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(transactionConsumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        try {
            consumer.subscribe(topic, "*");
            consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
                try {
                    MessageExt msg = list.get(0);
                    String tag = msg.getTags();
                    String body = new String(msg.getBody(), StandardCharsets.UTF_8.name());
                    System.out.println(transactionConsumerGroup+" receive msg.tag:"+tag+",body:"+body);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer.start();
            System.out.println(transactionConsumerGroup+" started.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
