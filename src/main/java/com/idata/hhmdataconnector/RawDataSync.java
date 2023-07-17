package com.idata.hhmdataconnector;

import cn.hutool.core.date.DateUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import java.sql.SQLException;
import static com.idata.hhmdataconnector.ReadData.getRawDF;
import static com.idata.hhmdataconnector.utils.connectionUtil.hhm_mysqlProperties;
import static com.idata.hhmdataconnector.utils.tableUtil.createMySQLTable;

public class RawDataSync {
    public static void main(String[] args) throws SQLException {
        String dataSourceName ="CF";
        String tableName ="T_SJKJ_RMTJ_AJBL";
        String raw = "raw";
        String timeField = "SLRQ";
        String begintime = DateUtil.beginOfDay(DateUtil.lastMonth()).toString("yyyy-MM-dd HH:mm:ss");
        String endtime = DateUtil.beginOfDay(DateUtil.lastMonth()).toString("yyyy-MM-dd HH:mm:ss");
        syncData(dataSourceName,tableName,raw,timeField,begintime,endtime);
    }

    public static void syncData(String dataSourceName, String tableName , String raw, String timeField, String beginTime,String endTime) throws SQLException {

        SparkSession spark = SparkSession.builder()
                .appName("RawDataSync")
                .master("local[20]")
                .getOrCreate();
//        System.out.println("==========================================================================================================");
        Dataset<Row> rawDF = getRawDF(spark, tableName, dataSourceName, timeField, beginTime,endTime, raw);
//        rawDF.printSchema();
//        System.out.println("============================================================================================================");
//        rawDF.show();
//        /*
//          获取Oracle表的字段结构
//         */
//        StructType rawTableSchema = rawDF.schema();
//
//        /*
//          创建MySQL表的字段结构
//         */
//        createMySQLTable(spark, tableName, rawTableSchema);

        /*
          将原始表数据写入MySQL表中
         */
        rawDF
                .repartition(20)
                .write()
                .mode("append")
                .jdbc(DataSource.HHM.getUrl(), tableName, hhm_mysqlProperties());

        spark.close();
    }

}
