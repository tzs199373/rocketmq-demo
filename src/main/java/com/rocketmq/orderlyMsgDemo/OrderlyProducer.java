package com.rocketmq.orderlyMsgDemo;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class OrderlyProducer {
    private static final String producerGroup = "orderlyProducerGroup";
    private static String namesrvAddr = "127.0.0.1:9876";
    private static final String topic = "orderTopic";

    public static void main(String[] args) {
        try {
            DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
            producer.setNamesrvAddr(namesrvAddr);
            producer.setSendMsgTimeout(5000);
            producer.setRetryTimesWhenSendFailed(3);
            producer.start();
            String[] tags = new String[]{"创建订单", "支付", "发货", "收货", "五星好评"};
            for (int i = 5; i < 25; i++) {
                int orderId = i / 5;
                Message msg = new Message(topic, tags[i % tags.length], "uniqueId:" + i,
                        ("order_" + orderId + " " + tags[i % tags.length]).getBytes(StandardCharsets.UTF_8.name()));
                SendResult sendResult = producer.send(msg,(List<MessageQueue> mqs, Message message, Object arg)->{
                    //此刻arg == orderId,可以保证是每个订单进入同一个队列
                    Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    return mqs.get(index);
                }, orderId);
                System.out.println(sendResult);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
