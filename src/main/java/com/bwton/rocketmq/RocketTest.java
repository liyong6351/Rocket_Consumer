package com.bwton.rocketmq;

import com.bwton.rocketmq.config.ConfigModel;
import com.bwton.rocketmq.consumer.ConsumerExecutor;
import com.bwton.rocketmq.enums.MsgTypeEnum;
import com.bwton.rocketmq.utils.MapUtils;
import io.netty.util.internal.StringUtil;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName com.bwton.rocketmq.utils.MapUtils
 * @Description 用于Jmetter调用进行测试写入RocketMq的测试类
 * @Author liyong
 * @Date 2018-12-11 19:16
 * @Version 0.0.1
 **/
public class RocketTest {

    public static void main(String[] args) {
        Map<String, Object> map = tranPara2Model(args);
        if (MapUtils.isValueEmpty(map, "msg")) {
            System.out.println(map.get("msg"));
        } else {
            ConsumerExecutor.executor((ConfigModel) map.get("data"));
        }
    }

    /**
     * 参数1 表示发送消息的类型(0无序消息 1有序消息 2延时消息(延时为随机的10秒之内的值))
     * 参数2 表示发送的TopicId
     * 参数3 表示发送的 Tag
     * 参数4 表示发送消息的条数 默认一次执行10000条消息
     * 参数5 表示发送消息的长度 默认一次发送800字符的消息
     * 参数6 表示配置的NameServer 示例:"10.1.54.121:9876;10.1.54.122:9876"
     *
     * @param args 运行消息
     */
    private static Map<String, Object> tranPara2Model(String[] args) {
        Map<String, Object> result = new HashMap<>();
        String msg = checkPara(args);
        result.put("msg", msg);
        if (StringUtil.isNullOrEmpty(msg)) {
            result.put("data", assemblyModel(args));
        }
        return result;
    }

    /**
     * 参数1 表示配置的NameServer 示例:"10.1.54.121:9876;10.1.54.122:9876"
     * 参数2 表示发送消息的类型(0无序消息 1有序消息 2延时消息(延时为随机的10秒之内的值))
     * 参数3 表示发送的TopicId
     * 参数4 表示发送的 Tag
     * 参数5 表示发送消息的条数 默认一次执行10000条消息
     * 参数6 表示发送消息的长度 默认一次发送800字符的消息
     *
     * @param args 运行消息
     */
    private static ConfigModel assemblyModel(String[] args) {
        ConfigModel model = new ConfigModel();
        if (args.length >= 5) {
            model.setNameServer(args[0]);
            model.setGroupName(args[1]);
            model.setMsgTypeEnum(MsgTypeEnum.getEnum(Integer.valueOf(args[2])));
            model.setTopicId(args[3]);
            model.setTags(args[4].split(":"));
        }
        if (args.length >= 7) {
            model.setMsgCount(Integer.valueOf(args[5]));
            model.setMsgLength(Integer.valueOf(args[6]));
        } else if (args.length == 6) {
            model.setMsgCount(Integer.valueOf(args[5]));
        }
        return model;
    }

    /**
     * 参数1 表示配置的NameServer 示例:"10.1.54.121:9876;10.1.54.122:9876"
     * 参数2 表示配置的groupName
     * 参数2 表示发送消息的类型(0无序消息 1有序消息 2延时消息(延时为随机的10秒之内的值))
     * 参数3 表示发送的TopicId
     * 参数4 表示发送的 Tag
     * 参数5 表示发送消息的条数 默认一次执行10000条消息
     * 参数6 表示发送消息的长度 默认一次发送800字符的消息
     *
     * @param args 运行消息
     */
    private static String checkPara(String[] args) {
        String result = "";
        if (args == null) {
            return "没有传入参数";
        } else {
            if (args.length < 5) {
                return "至少传入5个参数";
            } else {
                String nameServer = args[0];
                String[] nameServers = nameServer.split(";");
                for (String server : nameServers) {
                    String[] servers = server.split(":");
                    if (servers.length < 2) {
                        return "nameServer传递有问题，请重新传入";
                    } else if (!"9876".equals(servers[1])) {
                        return "nameServer的端口不是9876，请重新传入";
                    }
                }


                String msgType = args[2];
                if (NumberUtils.isDigits(msgType)) {
                    MsgTypeEnum msgTypeEnum = MsgTypeEnum.getEnum(Integer.valueOf(msgType));
                    if (msgTypeEnum == null) {
                        return "消息类型不对，请重新输入";
                    }
                } else {
                    return "传入的消息类型不对，请重新输入";
                }

                if (args.length >= 6) {
                    String msgNum = args[5];
                    if (!NumberUtils.isDigits(msgNum)) {
                        return "输入的发送条数不是整数，请重新输入";
                    }
                }

                if (args.length >= 7) {
                    String msgLength = args[6];
                    if (!NumberUtils.isDigits(msgLength)) {
                        return "输入的消息最大长度不是整数，请重新输入";
                    }
                }
            }
        }
        return result;
    }
}
