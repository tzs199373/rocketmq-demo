package com.rocketmq.orderlyMsgDemo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class OrderlyConsumer {
    private static final String consumerGroup = "orderlyConsumerGroup";
    private static String namesrvAddr = "127.0.0.1:9876";
    private static final String topic = "orderTopic";

    public static void main(String[] args) {
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.subscribe(topic, "*");
            // 实现了MessageListenerOrderly表示一个队列只会被一个线程取到, 第二个线程无法访问这个队列,MessageListenerOrderly默认单线程
            consumer.registerMessageListener((List<MessageExt> msgs, ConsumeOrderlyContext context)-> {
                    try {
                        MessageExt msg = msgs.get(0);
                        String tag = msg.getTags();
                        String body = new String(msg.getBody(), StandardCharsets.UTF_8.name());
                        System.out.println(consumerGroup+" receive msg.tag:"+tag+",body:"+body);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            );
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
