package com.idata.hhmdataconnector;

import com.idata.hhmdataconnector.enums.DataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class ReadData {
    public static Dataset<Row> getRawDF(SparkSession sparkSession, String sourceTableName, String sourceName,String timeField,String beginTime,String endTime,String rawFlag){
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
        System.out.println(dataSource+dataSource.getUser()+dataSource.getPassword());
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

        if(sourceName.equals("HHM")||sourceName.equals("JMLT_ORIGIN")||sourceName.equals("CF_ORIGIN")){
            driver = mysqlDriver;
        }else {
            driver = oracleDriver;
        }
        System.out.println(dataSource.getUrl()+sourceTableName+dataSource.getUser()+dataSource.getPassword());
        Dataset<Row> rawDF = sparkSession
                .read()
                .option("driver",driver)
                .jdbc(dataSource.getUrl(), sourceTableName, origin_properties)
                .toDF();

        if(rawFlag.equals("raw")){
            //初始化同步原始所有数据
            return rawDF
                    .where(rawDF.col(timeField).$greater(beginTime))
                    .where(rawDF.col(timeField).$less(endTime));
        }else {
            return rawDF;
        }
    }
}
