package com.rocketmq.springRocketmq;

import io.github.rhwayfun.springboot.rocketmq.starter.common.AbstractRocketMqConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class SpringConsumer extends AbstractRocketMqConsumer<DemoMqTopic, DemoMqContent> {
    @Override
    public boolean consumeMsg(DemoMqContent content, MessageExt msg) {
        System.out.println(new Date() + ", " + content);
        return true;
    }

    @Override
    public Map<String, Set<String>> subscribeTopicTags() {
        Map<String, Set<String>> map = new HashMap<>();
        Set<String> tags = new HashSet<>();
        tags.add("spring-tag");
        map.put("spring-topic", tags);
        return map;
    }

    @Override
    public String getConsumerGroup() {
        return "SpringConsumerGroup";
    }
}
