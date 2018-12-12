package com.bwton.rocketmq.utils;

import java.util.Random;

/**
 * @ClassName com.bwton.rocketmq.utils.StringUtils
 * @Description 定制自己使用的String工具类
 * @Author liyong
 * @Date 2018-12-12 10:23
 * @Version 0.0.1
 **/
public class StringUtils extends org.apache.commons.lang3.StringUtils {
    private static String allCharacter="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /**
     * @Author liyong
     * @Description 生成定长的随机字符串
     * @Date  10:29
     * @Param [length]
     * @return java.lang.String
     **/
    public static String getRandomFixedLengthString(int length){
        Random random=new Random();
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<length;i++){
            int num=random.nextInt(allCharacter.length());
            sb.append(allCharacter.charAt(num));
        }
        return sb.toString();
    }
}
