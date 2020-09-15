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
    //执行本地事务
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        //从消息中获取业务唯一ID
        String bizUniNo = msg.getUserProperty("bizUniNo");
        System.out.println("executeLocalTransaction bizUniNo:"+bizUniNo);
        // 将bizUniNo入库,表名:t_message_transaction,表结构bizUniNo (主键), 业务类型。


        if(bizUniNo.equals("2")){//模拟回滚
            System.out.println("bizUniNo="+bizUniNo+" 的消息回滚");
            //生产者发送三条消息,这里模拟bizUniNo=1的消息需要回滚
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        //可以直接commit或rollback，如果担心mq因网络故障收不到，推荐发送unknow，这样mq会主动来调用未决事务方法
        return LocalTransactionState.UNKNOW;
    }

    @Override
    //未决事务
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        // 从数据库查询t_message_transaction表,如果该表中存在记录,则提交
        String bizUniNo = msg.getUserProperty("bizUniNo");
        System.out.println("checkLocalTransaction bizUniNo:"+bizUniNo);

        if(bizUniNo.equals("1")){//模拟回滚
            System.out.println("bizUniNo="+bizUniNo+" 的消息回滚");
            //生产者发送三条消息,这里模拟bizUniNo=1的消息需要回滚
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }


        // 然后查询t_message_transaction 表,是否存在bizUniNo,如果存在,则返回COMMIT_MESSAGE,
        if (query(bizUniNo) > 0) {
            return LocalTransactionState.COMMIT_MESSAGE;
        }

        // 不存在,则记录查询次数,未超过次数,返回UNKNOW,超过次数,返回ROLLBACK_MESSAGE
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


