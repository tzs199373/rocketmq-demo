package com.rocketmq.simpleMsgDemo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class SimpleConsumer {
    private static final String consumerGroup = "simpleConsumerGroup";
    private static String namesrvAddr = "127.0.0.1:9876";
    private DefaultMQPushConsumer consumer;
    private static final String topic = "simpleTopic";

    public void initConsumerClient(){
        if (consumer == null) {
            consumer = new DefaultMQPushConsumer(consumerGroup);
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
                try {
                    MessageExt msg = list.get(0);
                    String tag = msg.getTags();
                    String body = new String(msg.getBody(), StandardCharsets.UTF_8.name());
                    System.out.println(consumerGroup+" receive msg.tag:"+tag+",body:"+body);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
        }
        try {
            consumer.subscribe(topic, "*");
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
