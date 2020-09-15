package com.rocketmq.transactionMsgDemo;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;


public class TransactionListenerImpl implements TransactionListener {
    private ConcurrentHashMap<String, Integer> countHashMap = new ConcurrentHashMap<>();
    private final static int MAX_COUNT = 5;

    @Override
    //ִ�б�������
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        //����Ϣ�л�ȡҵ��ΨһID
        String bizUniNo = msg.getUserProperty("bizUniNo");
        System.out.println("executeLocalTransaction bizUniNo:"+bizUniNo);
        // ��bizUniNo���,����:t_message_transaction,��ṹbizUniNo (����), ҵ�����͡�


        if(bizUniNo.equals("2")){//ģ��ع�
            System.out.println("bizUniNo="+bizUniNo+" ����Ϣ�ع�");
            //�����߷���������Ϣ,����ģ��bizUniNo=1����Ϣ��Ҫ�ع�
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        //����ֱ��commit��rollback���������mq����������ղ������Ƽ�����unknow������mq������������δ�����񷽷�
        return LocalTransactionState.UNKNOW;
    }

    @Override
    //δ������
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        // �����ݿ��ѯt_message_transaction��,����ñ��д��ڼ�¼,���ύ
        String bizUniNo = msg.getUserProperty("bizUniNo");
        System.out.println("checkLocalTransaction bizUniNo:"+bizUniNo);

        if(bizUniNo.equals("1")){//ģ��ع�
            System.out.println("bizUniNo="+bizUniNo+" ����Ϣ�ع�");
            //�����߷���������Ϣ,����ģ��bizUniNo=1����Ϣ��Ҫ�ع�
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }


        // Ȼ���ѯt_message_transaction ��,�Ƿ����bizUniNo,�������,�򷵻�COMMIT_MESSAGE,
        if (query(bizUniNo) > 0) {
            return LocalTransactionState.COMMIT_MESSAGE;
        }

        // ������,���¼��ѯ����,δ��������,����UNKNOW,��������,����ROLLBACK_MESSAGE
        return rollBackOrUnown(bizUniNo);
    }

    public int query(String bizUniNo) {
        return 1; //select count(1) from t_message_ transaction a where a.bizUniNo=#{bizUniNo}
    }

    public LocalTransactionState rollBackOrUnown(String bizUniNo){
        Integer num = countHashMap.get(bizUniNo);
        if(num != null && ++num > MAX_COUNT){
            countHashMap.remove(bizUniNo);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        if(num == null){
            num = new Integer(1);
        }
        countHashMap.put(bizUniNo,num);
        return LocalTransactionState.UNKNOW;
    }
}


