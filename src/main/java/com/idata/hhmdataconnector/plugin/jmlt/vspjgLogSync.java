package com.idata.hhmdataconnector.plugin.jmlt;

import com.idata.hhmdataconnector.DataSource;
import com.idata.hhmdataconnector.model.hhm.t_mediation_case;
import com.idata.hhmdataconnector.model.hhm.t_mediation_case_log;
import com.idata.hhmdataconnector.model.jmlt.V_SPJG;
import com.idata.hhmdataconnector.utils.DateUtils;
import org.apache.spark.sql.*;
import scala.Function1;
import java.io.Serializable;
import static com.idata.hhmdataconnector.ReadData.getRawDF;
import static com.idata.hhmdataconnector.utils.connectionUtil.hhm_mysqlProperties;

/**
 * @description: some desc
 * @author: xiehaotian
 * @date: 2023/7/10 9:45
 */
public class vspjgLogSync {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("vspjgLogSync")
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

        //获取来源表数据
        Dataset<Row> rawDF = getRawDF(spark, tableName, dataSourceName,"","","");
        Dataset<Row> rowDataset = rawDF;

        //定义数据源对象
        Dataset<V_SPJG> rowDF = rowDataset.as(Encoders.bean(V_SPJG.class));

        //关联case表获取id
        Dataset<t_mediation_case> caseDF = getRawDF(spark, "t_mediation_case", "HHM","","","").as(Encoders.bean(t_mediation_case.class));

        Dataset<Row> joinDF = rowDF.join(caseDF, rowDF.col("LSH").equalTo(caseDF.col("resource_id")), "left");

        joinDF.show(10);
        //转化为目标表结构
        Dataset<t_mediation_case_log> tcDF = joinDF
                .map(new ConvertToTMediationLog(), Encoders.bean(t_mediation_case_log.class));

        tcDF
                .repartition(20)
                .write()
                .mode(SaveMode.Append)
                .jdbc(DataSource.HHM.getUrl(), targetTableName, hhm_mysqlProperties());
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
