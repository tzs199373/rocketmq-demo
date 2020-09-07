package com.rocketmq.simpleRocketmq;

public class SimpleProducerStarter {
    public static void main(String[] args) {
        SimpleProducer producer = new SimpleProducer();
        producer.initProducerClient();
        producer.sendMsg("simple-topic","tags","你好啊，我是SimpleProducer发出的消息！");
    }
}
