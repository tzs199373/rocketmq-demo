package com.rocketmq.springRocketmq;


import io.github.rhwayfun.springboot.rocketmq.starter.common.DefaultRocketMqProducer;
import org.apache.rocketmq.common.message.Message;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Timer;
import java.util.TimerTask;

@Component
public class SpringProducer {
    @Resource
    private DefaultRocketMqProducer producer;

    @PostConstruct
    public void execute() {
        DemoMqContent content = new DemoMqContent();
        content.setId(1);
        content.setDesc("你好啊，我是SpringProducer发出的消息！");
        Message msg = new Message("spring-topic", "spring-tag", content.toString().getBytes(StandardCharsets.UTF_8));
        boolean sendResult = producer.sendMsg(msg);
        System.out.println("SpringProducer send msg:"+msg+",发送结果:"+sendResult);
    }
}




