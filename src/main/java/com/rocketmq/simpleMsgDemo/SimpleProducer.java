package com.rocketmq.simpleMsgDemo;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

public class SimpleProducer {
    private static final String producerGroup = "simpleProducerGroup";
    private static String namesrvAddr = "127.0.0.1:9876";
    private static final String topic = "simpleTopic";


    public static void main(String[] args){
        try {
            DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
            producer.setNamesrvAddr(namesrvAddr);
            producer.setSendMsgTimeout(5000);
            producer.start();
            Message msg = new Message(topic,"tags","你好啊，我是SimpleProducer发出的消息！".getBytes(StandardCharsets.UTF_8.name()));
            SendResult sendResult = producer.send(msg);
            System.out.println(sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
