package com.idata.hhmdataconnector.plugin.cf;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.DataSource;
import com.idata.hhmdataconnector.model.hhm.t_mediation_case;
import com.idata.hhmdataconnector.model.hhm.t_mediation_case_log;
import com.idata.hhmdataconnector.utils.DateUtils;
import org.apache.spark.sql.*;
import scala.Function1;
import java.io.Serializable;
import static com.idata.hhmdataconnector.ReadData.getRawDF;
import static com.idata.hhmdataconnector.utils.connectionUtil.hhm_mysqlProperties;

/**
 * @description: some desc
 * @author: xiehaotian
 * @date: 2023/7/10 16:18
 */
public class tjjlLogSync {
    public static void main(String[] args) {
//        String begintime = DateUtil.beginOfDay(DateUtil.lastMonth()).toString("yyyy-MM-dd");
//        String raw = "raw";
//        dataSync(begintime,raw);
    }
    public static void dataSync(String beginTime,String endTime, String raw){
        SparkSession spark = SparkSession.builder()
                .appName("tjjlLogSync")
                .master("local[20]")
                .getOrCreate();
        /*
          dataSourceName包括如下
          1、JMLT
          2、CF
          3、HHM
         */
        String dataSourceName = "CF";//args[0];
        String tableName = "T_SJKJ_RMTJ_TJJL";//args[1];
        String targetTableName = "t_mediation_case_log_test";
        String beginTimeStr = DateUtil.parse(beginTime).toString("yyyy-MM-dd");
        String endTimeStr = DateUtil.parse(endTime).toString("yyyy-MM-dd");
        //获取来源表数据
        Dataset<Row> rawDF0 = getRawDF(spark, tableName, dataSourceName,"TJRQ",beginTimeStr,endTimeStr,raw)
                .withColumnRenamed("TJRQ", "create_time")
                .withColumnRenamed("TJJL", "log_description")
                .select("AJBH","create_time","log_description");
//        rawDF0.printSchema();

        Dataset<Row> rawDF1 = getRawDF(spark, "T_SJKJ_RMTJ_DCJL", dataSourceName,"DCRQ",beginTimeStr,endTimeStr,raw)
                .withColumnRenamed("DCRQ", "create_time")
                .withColumnRenamed("DCJL", "log_description")
                .select("AJBH","create_time","log_description");
//        rawDF1.printSchema();

        Dataset<Row> rawDF = rawDF0.union(rawDF1);
        Dataset<Row> rowDataset = rawDF;

        //定义数据源对象
//        Dataset<T_SJKJ_RMTJ_TJJL> rowDF = rowDataset.as(Encoders.bean(T_SJKJ_RMTJ_TJJL.class));

        //关联case表获取id
        Dataset<t_mediation_case> caseDF = getRawDF(spark, "t_mediation_case_test", "HHM","","","","").as(Encoders.bean(t_mediation_case.class));

        Dataset<Row> joinDF = rowDataset.join(caseDF, rowDataset.col("AJBH").equalTo(caseDF.col("case_num")), "left");

        //转化为目标表结构
        Dataset<t_mediation_case_log> tcDF = joinDF
                .map(new ConvertToTMediationLog(), Encoders.bean(t_mediation_case_log.class));
//        tcDF.show();
        tcDF
                .repartition(20)
                .write()
                .mode(SaveMode.Append)
                .jdbc(DataSource.HHM.getUrl(), targetTableName, hhm_mysqlProperties());
        spark.close();
    }
    public static class ConvertToTMediationLog implements Function1<Row, t_mediation_case_log>, Serializable {
        @Override
        public t_mediation_case_log apply(Row vspjg) {
            t_mediation_case_log logEntity = new t_mediation_case_log();
            if(vspjg.getAs("id") != null){
                logEntity.setCase_id(Long.parseLong(vspjg.getAs("id").toString()));
            }
            if(vspjg.getAs("create_time") != null){
                logEntity.setCreate_time(vspjg.getAs("create_time"));
                logEntity.setUpdate_time(DateUtils.strToTsSFM(vspjg.getAs("create_time").toString()));
            }else {
                logEntity.setCreate_time(DateUtil.now());
                logEntity.setUpdate_time(DateUtil.now());
            }
            if(vspjg.getAs("log_description") != null){
                logEntity.setLog_description(vspjg.getAs("log_description").toString());
            }

            // 设置其他属性
            return logEntity;
        }
    }
}
