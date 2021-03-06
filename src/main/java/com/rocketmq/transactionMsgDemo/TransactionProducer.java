package com.rocketmq.transactionMsgDemo;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

public class TransactionProducer {
    private static final String transactionProducerGroup = "transactionProducerGroup";
    private static String namesrvAddr = "127.0.0.1:9876";
    private static final String topic = "transactionTopic";

    public static void main(String[] args) {
        try {
            TransactionListener transactionListenerImpl = new TransactionListenerImpl();
            TransactionMQProducer producer = new TransactionMQProducer(transactionProducerGroup);
            producer.setNamesrvAddr(namesrvAddr);
            producer.setSendMsgTimeout(5000);
            producer.setTransactionListener(transactionListenerImpl);
            producer.start();
            for (int i = 0; i < 3; i++) {
                Message msg = new Message(topic, "tags",
                        ("Hello RocketMQ " + i).getBytes(StandardCharsets.UTF_8.name()));
                msg.putUserProperty("bizUniNo",i+"");
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.println(sendResult);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}


