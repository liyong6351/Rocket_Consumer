package com.bwton.rocketmq.enums;

/**
 * @ClassName com.bwton.rocketmq.enums.MsgTypeEnum
 * @Description TODO
 * @Author mac
 * @Date 2018-12-11 18:56
 * @Version 0.0.1
 **/
public enum MsgTypeEnum {
    COMMON("无序非延迟", 1),
    ORDERED("有序非延迟", 2),
    COMMON_DELAY("无序延迟", 3),
    ORDERED_DELAY("有序延迟", 4),
    UN_KNOWN("未知", 0);

    // 成员变量
    private String name;
    private int index;

    // 构造方法
    MsgTypeEnum(String name, int index) {
        this.name = name;
        this.index = index;
    }

    // 普通方法  
    public static MsgTypeEnum getEnum(int index) {
        for (MsgTypeEnum c : MsgTypeEnum.values()) {
            if (c.getIndex() == index){
                return c;
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    public int getIndex() {
        return index;
    }
}