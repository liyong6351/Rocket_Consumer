package com.bwton.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;

/**
 * @ClassName com.bwton.rocketmq.consumer.CommonConsumer
 * @Description 无序消息的消费
 * @Author liyong
 * @Date 2018-12-12 11:10
 * @Version 0.0.1
 **/
class CommonConsumer {

    static void executor(DefaultMQPushConsumer consumer) {
        //设置一个Listener，主要进行消息的逻辑处理
        //注意这里使用的是MessageListenerConcurrently这个接口
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {

            System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs);

            //返回消费状态
            //CONSUME_SUCCESS 消费成功
            //RECONSUME_LATER 消费失败，需要稍后重新消费
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        try {
            //调用start()方法启动consumer
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        System.out.println("Consumer Started.");
    }
}