package com.idata.hhmdataconnector;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.enums.DataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.sql.SQLException;
import static com.idata.hhmdataconnector.ReadData.getRawDF;
import static com.idata.hhmdataconnector.utils.tableUtil.createMySQLTable;

public class RawDataSync {

    public static void main(String[] args) throws SQLException {
        String dataSourceName ="CF";
        String tableName ="T_SJKJ_RMTJ_AJBL";
        String raw = "raw";
        String timeField = "SLRQ";
        String begintime = DateUtil.beginOfDay(DateUtil.lastMonth()).toString("yyyy-MM-dd HH:mm:ss");
        String endtime = DateUtil.beginOfDay(DateUtil.lastMonth()).toString("yyyy-MM-dd HH:mm:ss");
        MultisyncData("CF","CF_ORIGIN","T_SJKJ_RMTJ_TJWYH","",timeField,begintime,endtime);
    }

    public static void syncData(String dataSourceName, String tableName , String raw, String timeField, String beginTime,String endTime) throws SQLException {

        SparkSession spark = SparkSession.builder()
                .appName("RawDataSync"+tableName)
                .master("local[20]")
                .getOrCreate();

        Dataset<Row> rawDF = getRawDF(spark, tableName, dataSourceName, timeField, beginTime,endTime, raw);

        /*
          获取Oracle表的字段结构
         */
        StructType rawTableSchema = rawDF.schema();

        /*
          创建MySQL表的字段结构
         */
        createMySQLTable(spark, tableName, rawTableSchema);

        String url = "";
        String name = "";
        String psword = "";
        for (DataSource dataSource : DataSource.values()) {
            if (dataSource.name().equalsIgnoreCase(dataSourceName)) {
                url =  dataSource.getUrl();
                name = dataSource.getUser();
                psword = dataSource.getPassword();
            }
        }

        java.util.Properties properties = new java.util.Properties();
        properties.put("user", "root");
        properties.put("password", "idata@2023");
        properties.put("driver","com.mysql.jdbc.Driver");
        /*
          将原始表数据写入MySQL表中
         */
        rawDF
                .repartition(20)
                .write()
                .mode(SaveMode.Append)
                .jdbc(url, tableName, properties);

        spark.close();
    }
    public static void MultisyncData(String dataSourceName,String targetDataSourceName, String tableName , String raw, String timeField, String beginTime,String endTime) throws SQLException {

        SparkSession spark = SparkSession.builder()
                .appName("RawDataSync"+tableName)
                .master("local[20]")
                .getOrCreate();
        Dataset<Row> rawDF = getRawDF(spark, tableName, dataSourceName, timeField, beginTime,endTime, raw);
//        rawDF.show(10);
//        /*
//          获取Oracle表的字段结构
//         */
//        StructType rawTableSchema = rawDF.schema();
//
//        /*
//          创建MySQL表的字段结构
//         */
//        createMySQLTable(spark, tableName, rawTableSchema);

        String url = "";
        String name = "";
        String psword = "";
        for (DataSource dataSource : DataSource.values()) {
            if (dataSource.name().equalsIgnoreCase(targetDataSourceName)) {
                url =  dataSource.getUrl();
                name = dataSource.getUser();
                psword = dataSource.getPassword();
            }
        }

        java.util.Properties properties = new java.util.Properties();
        properties.put("user", "root");
        properties.put("password", "idata@2023");
        properties.put("driver","com.mysql.jdbc.Driver");
        /*
          将原始表数据写入MySQL表中
         */
        System.out.println(url);
        rawDF
                .repartition(20)
                .write()
                .mode(SaveMode.Append)
                .jdbc(url, tableName, properties);

        spark.close();
    }
}
