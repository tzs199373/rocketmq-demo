package com.rocketmq.springRocketmq;

import io.github.rhwayfun.springboot.rocketmq.starter.constants.RocketMqTopic;

public class DemoMqTopic implements RocketMqTopic {

    public String getTopic() {
        return "spring-topic";
    }
}
