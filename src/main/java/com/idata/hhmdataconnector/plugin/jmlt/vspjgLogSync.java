package com.idata.hhmdataconnector.plugin.jmlt;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.enums.DataSource;
import com.idata.hhmdataconnector.model.hhm.t_mediation_case;
import com.idata.hhmdataconnector.model.hhm.t_mediation_case_log;
import com.idata.hhmdataconnector.model.jmlt.V_SPJG;
import com.idata.hhmdataconnector.utils.DateUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import scala.Function1;
import java.io.Serializable;
import static com.idata.hhmdataconnector.ReadData.getRawDF;
import static com.idata.hhmdataconnector.utils.connectionUtil.hhm_mysqlProperties;
import static com.idata.hhmdataconnector.utils.tableUtil.deleteTableBeforeInsert;

/**
 * @description: some desc
 * @author: xiehaotian
 * @date: 2023/7/10 9:45
 */
public class vspjgLogSync {
    public static void main(String[] args) {
//        String begintime = DateUtil.beginOfDay(DateUtil.lastMonth()).toString("yyyyMMddHHmmss");
//        String raw = "raw";
//        dataSync(begintime,raw);
    }
    public static void dataSync(String beginTime,String endTime, String raw) {
        SparkConf conf = new SparkConf();
        conf.set("spark.driver.cores","4");  //设置driver的CPU核数
//        conf.set("spark.driver.maxResultSize","2g"); //设置driver端结果存放的最大容量，这里设置成为2G，超过2G的数据,job就直接放弃，不运行了
        conf.set("spark.driver.memory","4g");  //driver给的内存大小
        conf.set("spark.executor.memory","8g");// 每个executor的内存
        SparkSession spark = SparkSession.builder()
                .appName("vspjgLogSync:"+beginTime)
                .config(conf)
                .master("local[20]")
                .getOrCreate();
        /*
          dataSourceName包括如下
          1、JMLT
          2、CF
          3、HHM
         */
        String dataSourceName = "JMLT";//args[0];
        String tableName = "V_SPJG";//args[1];
        String targetTableName = "t_mediation_case_log";
        String beginTimeStr = DateUtil.parse(beginTime).toString("yyyyMMddHHmmss");
        String endTimeStr = DateUtil.parse(endTime).toString("yyyyMMddHHmmss");
        //获取来源表数据
        Dataset<Row> rawDF = getRawDF(spark, tableName, dataSourceName,"SPSJ",beginTimeStr,endTimeStr,raw);
        Dataset<Row> rowDataset = rawDF;

        //定义数据源对象
        Dataset<V_SPJG> rowDF = rowDataset.as(Encoders.bean(V_SPJG.class));

        //关联case表获取id
        Dataset<t_mediation_case> caseDF = getRawDF(spark, "t_mediation_case", "HHM","","","","")
                .as(Encoders.bean(t_mediation_case.class));

        Dataset<Row> joinDF = rowDF
                .join(caseDF, rowDF.col("LSH").equalTo(caseDF.col("resource_id")));


        //转化为目标表结构
        Dataset<t_mediation_case_log> tcDF = joinDF
                .map(new ConvertToTMediationLog(), Encoders.bean(t_mediation_case_log.class));
        tcDF.show(10);
        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(targetTableName, DataSource.HHM.getUrl(), DataSource.HHM.getUser(), DataSource.HHM.getPassword(), beginTimeStr,endTimeStr,"update_time","2");
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
            if(vspjg.getAs("SPSJ") != null){
                logEntity.setCreate_time(DateUtils.strToTsSFM(vspjg.getAs("SPSJ").toString()));
                logEntity.setUpdate_time(DateUtils.strToTsSFM(vspjg.getAs("SPSJ").toString()));
            }
            if(vspjg.getAs("CZMC") != null){
                logEntity.setLog_description(vspjg.getAs("CZMC").toString());
            }

            // 设置其他属性
            return logEntity;
        }
    }

}
