# rocketmq-demo
rocketmq的各种场景简单demo

## simpleMsgDemo包
生产者和消费者各一个启动类，开启rocketmq服务后，再启动即可，与spring容器无关

## orderlyMsgDemo包
顺序消费

## transactionMsgDemo包
事务消息:demo中发送三条消息，其中两条被回滚

## pullMsgDemo包
消息拉取模式，生产者用simpleMsgDemo包内即可，高版本rocketmq客户端,DefaultMQPullConsumer已废弃，可研究下拉取模式其他的api

## batchMsgDemo包
批量消息