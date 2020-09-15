package com.rocketmq.batchMsgDemo;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BatchProducer {
    private static final String producerGroup = "batchProducerGroup";
    private static String namesrvAddr = "127.0.0.1:9876";
    private static final String topic = "batchTopic";

    public static void main(String[] args){
        try {
            DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
            producer.setNamesrvAddr(namesrvAddr);
            producer.setSendMsgTimeout(5000);
            producer.start();

            List<Message> msgs = new ArrayList<>();
            Message msg1 = new Message(topic,"tags","你好啊，我是SimpleProducer发出的第1条消息！".getBytes(StandardCharsets.UTF_8.name()));
            Message msg2 = new Message(topic,"tags","你好啊，我是SimpleProducer发出的第2条消息！".getBytes(StandardCharsets.UTF_8.name()));
            Message msg3 = new Message(topic,"tags","你好啊，我是SimpleProducer发出的第3条消息！".getBytes(StandardCharsets.UTF_8.name()));
            msgs.add(msg1);
            msgs.add(msg2);
            msgs.add(msg3);
            SendResult sendResult = producer.send(msgs);
            System.out.println(sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
