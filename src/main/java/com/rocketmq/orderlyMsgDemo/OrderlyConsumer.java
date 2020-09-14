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
    private static final String consumerGroup = "OrderlyConsumerGroup";
    private static String namesrvAddr = "127.0.0.1:9876";

    public static void main(String[] args) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        try {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.subscribe("order-topic", "*");
            // ʵ����MessageListenerOrderly��ʾһ������ֻ�ᱻһ���߳�ȡ��, �ڶ����߳��޷������������,MessageListenerOrderlyĬ�ϵ��߳�
//            consumer.setConsumeThreadMin(3);
//            consumer.setConsumeThreadMax(6);
            consumer.registerMessageListener((List<MessageExt> msgs, ConsumeOrderlyContext context)-> {
                    try {
                        System.out.println("orderInfo: " + new String(msgs.get(0).getBody(), StandardCharsets.UTF_8.name()));
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
        System.out.println("OrderlyConsumer Started.");
    }

}
