package com.idata.hhmdataconnector.utils;

import cn.hutool.core.date.DateUtil;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @description: some desc
 * @author: xiehaotian
 * @date: 2023/5/29 9:47
 */
public class DateUtils {

    /**
     * 字符串时间转化类型(年月日)
     * @param s
     * @return
     */
    public static String strToTs(String s) throws ParseException {
        DateTimeFormatter sourceFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        DateTimeFormatter targetFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 将日期字符串解析为LocalDateTime对象
        LocalDateTime dateTime = LocalDateTime.parse(s, sourceFormatter);

        // 将LocalDateTime对象格式化为目标格式的字符串
        String formattedString = dateTime.format(targetFormatter);
        return formattedString;
    }

    /**
     * 字符串时间转化类型(年月日时分)
     * @param s
     * @return
     */
    public static String strToTsSF(String s) {

        // 定义日期字符串的格式和目标格式
        DateTimeFormatter sourceFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        DateTimeFormatter targetFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 将日期字符串解析为LocalDateTime对象
        LocalDateTime dateTime = LocalDateTime.parse(s, sourceFormatter);

        // 将LocalDateTime对象格式化为目标格式的字符串
        String formattedString = dateTime.format(targetFormatter);
        return formattedString;
    }

    /**
     * 字符串时间转化类型(年月日时分秒)
     * @param s
     * @return
     */
    public static String strToTsSFM(String s) {
        if(s.endsWith("60")){
            s= s.substring(0,12)+"59";
        }
        // 定义日期字符串的格式和目标格式
        DateTimeFormatter sourceFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        DateTimeFormatter targetFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 将日期字符串解析为LocalDateTime对象
        LocalDateTime dateTime = LocalDateTime.parse(s, sourceFormatter);

        // 将LocalDateTime对象格式化为目标格式的字符串
        String formattedString = dateTime.format(targetFormatter);
        return formattedString;
    }

    public static void main(String[] args) {
        String s = "20190928184760";
        if(s.endsWith("60")){
            s= s.substring(0,12)+"59";
        }
        System.out.println(DateUtil.date().toString());
    }
}
