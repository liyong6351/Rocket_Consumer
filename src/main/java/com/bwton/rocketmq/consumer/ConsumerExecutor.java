package com.bwton.rocketmq.consumer;

import com.bwton.rocketmq.config.ConfigModel;
import com.bwton.rocketmq.enums.MsgTypeEnum;
import com.bwton.rocketmq.producer.CommonMsg;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * @ClassName com.bwton.rocketmq.consumer.ConsumerExecutor
 * @Description 消费的执行类
 * @Author liyong
 * @Date 2018-12-12 11:19
 * @Version 0.0.1
 **/
public class ConsumerExecutor {
    public static void executor(ConfigModel model) {
        try {
            //声明并初始化一个consumer
            //需要一个consumer group名字作为构造方法的参数，这里为concurrent_consumer
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(model.getGroupName());
            //同样也要设置NameServer地址
            consumer.setNamesrvAddr(model.getNameServer());
            //这里设置的是一个consumer的消费策略
            //CONSUME_FROM_LAST_OFFSET 默认策略，从该队列最尾开始消费，即跳过历史消息
            //CONSUME_FROM_FIRST_OFFSET 从队列最开始开始消费，即历史消息（还储存在broker的）全部消费一遍
            //CONSUME_FROM_TIMESTAMP 从某个时间点开始消费，和setConsumeTimestamp()配合使用，默认是半个小时以前
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

            //设置consumer所订阅的Topic和Tag，*代表全部的Tag
            StringBuilder sb = new StringBuilder();
            if (model.getTags() != null) {
                for (String tag : model.getTags()) {
                    sb.append(tag);
                    sb.append("||");
                }
                consumer.subscribe(model.getTopicId(), sb.substring(0, sb.length() - 2));
            } else {
                consumer.subscribe(model.getTopicId(), "*");
            }

            if (MsgTypeEnum.ORDERED.equals(model.getMsgTypeEnum()) || MsgTypeEnum.ORDERED_DELAY.equals(model.getMsgTypeEnum())){
                OrderedConsumer.executor(consumer);
            }else if (MsgTypeEnum.COMMON.equals(model.getMsgTypeEnum()) || MsgTypeEnum.COMMON_DELAY.equals(model.getMsgTypeEnum())){
                CommonConsumer.executor(consumer);
            }
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
