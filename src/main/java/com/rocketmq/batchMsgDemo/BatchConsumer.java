package com.rocketmq.batchMsgDemo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.StandardCharsets;

public class BatchConsumer {
    private static final String consumerGroup = "batchConsumerGroup";
    private static String namesrvAddr = "127.0.0.1:9876";
    private static final String topic = "batchTopic";

    public static void main(String[] args){
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
                list.forEach((MessageExt msg)->{
                    try {
                        String tag = msg.getTags();
                        String body = new String(msg.getBody(), StandardCharsets.UTF_8.name());
                        System.out.println(consumerGroup+" receive msg.tag:"+tag+",body:"+body);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });
            consumer.subscribe(topic, "*");
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
