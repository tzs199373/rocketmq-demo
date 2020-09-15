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
    //pullģʽ��Ҫ����ά����Ϣ����
    //���������ϼ򵥣�ÿ���������ѷ���ᶪʧ��Ϣ���ȣ���������
    //�����������ά�������ؽ���ͬ����broker
    private static final Map<MessageQueue,Long> OFFSE_TABLE = new HashMap<>();

    private static final String consumerGroup = "pullConsumerGroup";
    private static String namesrvAddr = "127.0.0.1:9876";
    private static final String topic = "simpleTopic";

    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.start();
        // ��ָ��topic����ȡ������Ϣ����
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
        for(MessageQueue mq:mqs){
            try {
                // ��ȡ��Ϣ��offset��ָ����store�л�ȡ
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

    // �����ϴ����ѵ���Ϣ�±�
    private static void putMessageQueueOffset(MessageQueue mq,
                                              long nextBeginOffset) {
        OFFSE_TABLE.put(mq, nextBeginOffset);
    }

    // ��ȡ�ϴ����ѵ���Ϣ���±�
    private static Long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if(offset != null){
            return offset;
        }
        return 0l;
    }


}
