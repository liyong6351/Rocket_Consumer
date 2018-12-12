package com.bwton.rocketmq.config;

import com.bwton.rocketmq.enums.MsgTypeEnum;
import lombok.Data;

/**
 * @ClassName com.bwton.rocketmq.config.ConfigModel
 * @Description 将传入的参数抽象为一个model
 * @Author mac
 * @Date 2018-12-11 18:52
 * @Version 0.0.1
 **/
@Data
public class ConfigModel {
    private String groupName;
    private MsgTypeEnum msgTypeEnum;
    //topicId
    private String topicId;
    //发送的tags
    private String[] tags;
    //发送的消息条数
    private int msgCount=1000;
    //每条消息的长度
    private int msgLength;
    //mq的nameServer
    private String nameServer;
    //delay类型
    private Integer delayType;
}
