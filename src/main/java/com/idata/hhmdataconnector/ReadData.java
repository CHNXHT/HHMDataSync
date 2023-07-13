package com.idata.hhmdataconnector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class ReadData {
    public static Dataset<Row> getRawDF(SparkSession sparkSession, String sourceTableName, String sourceName){
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

        if (sourceName.equals("HHM")){
            Dataset<Row> rawDF = sparkSession
                    .read()
                    .option("driver",mysqlDriver)
                    .jdbc(dataSource.getUrl(), sourceTableName, origin_properties)
                    .toDF();
            return rawDF;
        }else {
//            origin_properties.setProperty("driver-class-name", "oracle.jdbc.driver.OracleDriver");
            Dataset<Row> rawDF = sparkSession
                    .read()
                    .option("driver",oracleDriver)
                    .jdbc(dataSource.getUrl(), sourceTableName, origin_properties)
                    .toDF();
            return rawDF;
        }

        /*
          原始表数据读取到Dataset对象中
         */

    }
}
