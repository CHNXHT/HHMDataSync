package com.idata.hhmdataconnector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class ReadData {
    public static Dataset<Row> getRawDF(SparkSession sparkSession, String sourceTableName, String sourceName,String timeField,String beginTime,String rawFlag){
         /*
        获取数据源配置信息参数
         */
        DataSource dataSource = null;
        for (DataSource ds : DataSource.values()) {
            if (ds.name().equalsIgnoreCase(sourceName)) {
                dataSource = ds;
                break;
            }
        }

        if (dataSource == null) {
            System.out.println("Unsupported data source: " + sourceName);
            sparkSession.stop();
            System.exit(1);
        }

        Properties origin_properties = new Properties();
        origin_properties.setProperty("user", dataSource.getUser());
        origin_properties.setProperty("password", dataSource.getPassword());

        String mysqlDriver = "com.mysql.jdbc.Driver";
        String oracleDriver = "oracle.jdbc.driver.OracleDriver";
        String driver = "";

        if(sourceName.equals("HHM")){
            driver = mysqlDriver;
        }else {
            driver = oracleDriver;
        }

        Dataset<Row> rawDF = sparkSession
                .read()
                .option("driver",driver)
                .jdbc(dataSource.getUrl(), sourceTableName, origin_properties)
                .toDF();

        if(rawFlag.equals("raw")){
            //初始化同步原始所有数据
            return rawDF.where(rawDF.col(timeField).$less(beginTime));
        }else if(rawFlag.equals("oneday")){
            //每天同步前一天数据（24-0）
            return rawDF.where(rawDF.col(timeField).$greater(beginTime));
        }else {
            return rawDF;
        }
    }
}
