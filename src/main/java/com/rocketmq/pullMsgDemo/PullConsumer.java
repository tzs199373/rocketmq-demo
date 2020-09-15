package com.rocketmq.pullMsgDemo;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

public class PullConsumer {
    //pull模式需要本地维护消息进度
    //本例做法较简单，每次重启消费服务会丢失消息进度，从零消费
    //生产考虑如何维护，本地进度同步到broker
    private static final Map<MessageQueue,Long> OFFSE_TABLE = new HashMap<>();

    private static final String consumerGroup = "pullConsumerGroup";
    private static String namesrvAddr = "127.0.0.1:9876";
    private static final String topic = "simpleTopic";

    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.start();
        // 从指定topic中拉取所有消息队列
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
        for(MessageQueue mq:mqs){
            try {
                // 获取消息的offset，指定从store中获取
                long offset = consumer.fetchConsumeOffset(mq,true);
                System.out.println("consumer from the queue:"+mq+":"+offset);
                while(true){
                    PullResult pullResult = consumer.pullBlockIfNotFound(mq, null,
                            getMessageQueueOffset(mq), 32);
                    putMessageQueueOffset(mq,pullResult.getNextBeginOffset());
                    switch(pullResult.getPullStatus()){
                        case FOUND:
                            List<MessageExt> messageExtList = pullResult.getMsgFoundList();
                            for (MessageExt msg : messageExtList) {
                                String body = new String(msg.getBody(), StandardCharsets.UTF_8.name());
                                System.out.println(body);
                            }
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            break;
                        case OFFSET_ILLEGAL:
                            break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        consumer.shutdown();
    }

    // 保存上次消费的消息下标
    private static void putMessageQueueOffset(MessageQueue mq,
                                              long nextBeginOffset) {
        OFFSE_TABLE.put(mq, nextBeginOffset);
    }

    // 获取上次消费的消息的下标
    private static Long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if(offset != null){
            return offset;
        }
        return 0l;
    }


}
