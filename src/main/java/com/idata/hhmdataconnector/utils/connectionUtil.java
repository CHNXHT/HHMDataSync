package com.idata.hhmdataconnector.utils;

public class connectionUtil {
    /**
     * oracle连接参数
     * @return
     */
    public static java.util.Properties jmlt_oracleProperties() {
        java.util.Properties properties = new java.util.Properties();
        properties.put("user", "U_MT");
        properties.put("password", "WYD5QzdzyyXG");
        return properties;
    }

    /**
     * mysql连接参数
     * @return
     */
    public static java.util.Properties hhm_mysqlProperties() {
        java.util.Properties properties = new java.util.Properties();
        properties.put("user", "root");
        properties.put("password", "idata@2023");
        properties.put("driver","com.mysql.jdbc.Driver");
        return properties;
    }

}
