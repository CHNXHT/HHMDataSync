package com.idata.hhmdataconnector.plugin.cf;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.enums.DataSource;
import com.idata.hhmdataconnector.model.hhm.t_mediation_participant;
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
 * @date: 2023/7/11 8:55
 */
public class ajblTJWYHParticpantSync {
    public static void main(String[] args) {
        String beginTime = "2017-07-01";//DateUtil.beginOfDay(DateUtil.lastMonth()).toString("yyyy-MM-dd HH:mm:ss");
        String endTime = "2023-07-23";//DateUtil.beginOfDay(DateUtil.yesterday()).toString("yyyy-MM-dd HH:mm:ss");
        dataSync(beginTime,endTime,"raw");
    }

    public static void dataSync(String beginTime,String endTime, String raw) {
        SparkConf conf = new SparkConf();
        conf.set("spark.driver.cores","4");  //设置driver的CPU核数
//        conf.set("spark.driver.maxResultSize","2g"); //设置driver端结果存放的最大容量，这里设置成为2G，超过2G的数据,job就直接放弃，不运行了
        conf.set("spark.driver.memory","4g");  //driver给的内存大小
        conf.set("spark.executor.memory","8g");// 每个executor的内存
        SparkSession spark = SparkSession.builder()
                .appName("ajblTJWYHParticpantSync:"+beginTime)
                .config(conf)
                .master("local[20]")
                .getOrCreate();
        /*
          dataSourceName包括如下
          1、JMLT
          2、CF
          3、HHM
         */
        String dataSourceName = "HHM";//args[0];
        String tableName = "t_mediation_case";//args[1];
        String targetTableName = "t_mediation_participant";
        String timeField = "SLRQ";
        String beginTimeStr = DateUtil.parse(beginTime).toString("yyyy-MM-dd HH:mm:ss");
        String endTimeStr = DateUtil.parse(endTime).toString("yyyy-MM-dd HH:mm:ss");
        //获取来源表数据
        Dataset<Row> rawCaseDF = getRawDF(spark, "t_mediation_case", "HHM","","","","")
                .select("id","case_num").withColumnRenamed("id","id1");
//        System.out.println("===========casecount========"+rawCaseDF.count());
        Dataset<Row> rawAJBLDF = getRawDF(spark, "T_SJKJ_RMTJ_AJBL", "CF",timeField,beginTimeStr,endTimeStr,raw)
                .select("SLDW","AJBH");
//        System.out.println("===========T_SJKJ_RMTJ_AJBLcount========"+rawCaseDF.count());
        Dataset<Row> rawTJWYHDF = getRawDF(spark, "T_SJKJ_RMTJ_TJWYH", "CF","","","","")
                .select("XZDQ","TWHMC");
//        System.out.println("===========T_SJKJ_RMTJ_TJWYHcount========"+rawCaseDF.count());
        Dataset<Row> AJBL_TJWYH_DF = rawAJBLDF
                .join(rawTJWYHDF, rawAJBLDF.col("SLDW").equalTo(rawTJWYHDF.col("TWHMC")),"left")
                .select("AJBH","XZDQ")
                .distinct();
//        System.out.println("===========AJBL_TJWYH_DF========"+rawCaseDF.count());

        Dataset<Row> caseJoinDF = AJBL_TJWYH_DF
                .join(rawCaseDF, rawCaseDF.col("case_num").equalTo(AJBL_TJWYH_DF.col("AJBH")),"left")
                .distinct();
//        System.out.println("===========caseJoinDF========"+rawCaseDF.count());
        Dataset<Row> organizationDF = getRawDF(spark, "t_organization", "HHM","","","","")
                .select("id","county","parent_id")
                .where("town = ''")
                .where("county !=''");

        Dataset<Row> organCountryDF = organizationDF.withColumn("county_6", organizationDF.col("county").substr(0, 6));
//        organCountryDF.show();
        Dataset<Row> resDF = caseJoinDF.join(organCountryDF, caseJoinDF.col("XZDQ").equalTo(organCountryDF.col("county_6")));

//        resDF.show();

        //转化为目标表结构
        Dataset<t_mediation_participant> tcDF = resDF
                .map(new convertToTMediationParticipant(), Encoders.bean(t_mediation_participant.class));

        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(targetTableName, DataSource.HHM.getUrl(),DataSource.HHM.getUser(), DataSource.HHM.getPassword(), beginTimeStr,endTimeStr,"update_time","2");
//        System.out.println("========================================start========================="+tcDF.count());
        tcDF
                .distinct()
                .repartition(20)
                .write()
                .mode(SaveMode.Append)
                .jdbc(DataSource.HHM.getUrl(), targetTableName, hhm_mysqlProperties());
        spark.close();
    }

    //暂时不确定
    public static class convertToTMediationParticipant implements Function1<Row, t_mediation_participant>, Serializable {
        @Override
        public t_mediation_participant apply (Row cas){
            t_mediation_participant tmediationcasepeople = new t_mediation_participant();
            //纠纷信息的主键id
            if(cas.getAs("id1") !=null){
                tmediationcasepeople.setCase_id(Long.parseLong(cas.getAs("id1").toString()));
            }
            //调解机构/调解员标识：1 调解机构、 2 调解员、3 协同调解员
            tmediationcasepeople.setFlag(1);
            //创建日期
            tmediationcasepeople.setCreate_time(DateUtil.now());
            //更新日期
            tmediationcasepeople.setUpdate_time(DateUtil.now());
            //纠纷机构id 766为肥东
            if(cas.getAs("parent_id") !=null){
                tmediationcasepeople.setOrg_id(Long.parseLong(cas.getAs("parent_id").toString()));
            }
//            tmediationcasepeople.setOrg_id();
//            案件流转参与者id 即能看到该纠纷数据的用户id 12310（叶秀）
//            tmediationcasepeople.setUser_id(12310L);

            return tmediationcasepeople;
        }
    }
}
