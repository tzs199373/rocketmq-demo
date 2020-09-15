package com.rocketmq.simpleRocketmq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class SimpleProducer {
    private final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

    private static final String producerGroup = "SimpleProducerGroup";

    private static String namesrvAddr = "127.0.0.1:9876";

    private DefaultMQProducer producer;


    public void initProducerClient(){
        if (producer == null) {
            producer = new DefaultMQProducer(producerGroup);
            producer.setNamesrvAddr(namesrvAddr);
            producer.setSendMsgTimeout(5000);
        }
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
            logger.error("producer start failed: {}",e.getMessage());
        }
    }

    public void sendMsg(String topic,String tags,String body){
        try {
            Message msg = new Message(topic,tags,body.getBytes(StandardCharsets.UTF_8.name()));
            SendResult result = producer.send(msg);
            System.out.println("SimpleProducer send msg:"+msg+",·¢ËÍ½á¹û:"+result.getSendStatus());
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
