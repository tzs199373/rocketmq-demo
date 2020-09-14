# rocketmq-demo
rocketmq的简单demo

## simpleRocketmq包
生产者和消费者各一个启动类，开启rocketmq服务后，再启动即可，与spring容器无关

## springRocketmq包
使用了io.github.rhwayfun的spring-boot-rocketmq-starter快速开发rocketmq业务，需要application.properties配置nameServer与producerGroup，个人感觉不怎么样，还不如在simpleRocketmq包加以改造，用以spring项目

