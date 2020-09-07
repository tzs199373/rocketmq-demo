package com.rocketmq.simpleRocketmq;

public class SimpleConsumerStarter {
    public static void main(String[] args) {
        SimpleConsumer consumer = new SimpleConsumer();
        consumer.initConsumerClient();
    }
}
