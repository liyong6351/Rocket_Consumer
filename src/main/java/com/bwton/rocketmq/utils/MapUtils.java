package com.bwton.rocketmq.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @ClassName com.bwton.rocketmq.utils.MapUtils
 * @Description Map的工具类
 * @Author mac
 * @Date 2018-12-11 19:16
 * @Version 0.0.1
 **/
public class MapUtils {

    public static boolean isValueEmpty(Map map, Object key) {
        boolean result = false;
        if (map != null && map.containsKey(key) && StringUtils.isEmpty(map.get(key).toString())) {
            result = true;
        }
        return result;
    }
}
