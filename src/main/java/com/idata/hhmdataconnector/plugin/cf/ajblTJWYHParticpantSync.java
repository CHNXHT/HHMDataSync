package com.idata.hhmdataconnector.plugin.cf;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.DataSource;
import com.idata.hhmdataconnector.model.hhm.t_mediation_participant;
import org.apache.spark.sql.*;
import scala.Function1;
import java.io.Serializable;
import static com.idata.hhmdataconnector.ReadData.getRawDF;
import static com.idata.hhmdataconnector.utils.connectionUtil.hhm_mysqlProperties;

/**
 * @description: some desc
 * @author: xiehaotian
 * @date: 2023/7/11 8:55
 */
public class ajblTJWYHParticpantSync {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ajblTJWYHParticpantSync")
                .master("local")
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

        //获取来源表数据
        Dataset<Row> rawCaseDF = getRawDF(spark, "t_mediation_case", "HHM").select("id","case_num");
        Dataset<Row> rawAJBLDF = getRawDF(spark, "T_SJKJ_RMTJ_AJBL", "CF").select("SLDW","AJBH");
        Dataset<Row> rawTJWYHDF = getRawDF(spark, "T_SJKJ_RMTJ_TJWYH", "CF").select("XZDQ","TWHMC");
        Dataset<Row> AJBL_TJWYH_DF = rawAJBLDF
                .join(rawTJWYHDF, rawAJBLDF.col("SLDW").equalTo(rawTJWYHDF.col("TWHMC")))
                .select("AJBH","XZDQ")
                .distinct();

        Dataset<Row> caseJoinDF = rawCaseDF
                .join(AJBL_TJWYH_DF, rawCaseDF.col("case_num").equalTo(AJBL_TJWYH_DF.col("AJBH")))
                .distinct();

        Dataset<Row> organizationDF = getRawDF(spark, "t_organization", "HHM")
                .select("id","county")
                .where("town = ''")
                .where("country !=''");

        Dataset<Row> organCountryDF = organizationDF.withColumn("county_6", organizationDF.col("county").substr(1, 6));

        Dataset<Row> resDF = caseJoinDF.join(organCountryDF, caseJoinDF.col("XZDQ").equalTo(organCountryDF.col("county_6")));


        //转化为目标表结构
        Dataset<t_mediation_participant> tcDF = resDF
                .map(new convertToTMediationParticipant(), Encoders.bean(t_mediation_participant.class));
        tcDF.show(10);
        tcDF
                .repartition(20)
                .write()
                .mode(SaveMode.Append)
                .jdbc(DataSource.HHM.getUrl(), targetTableName, hhm_mysqlProperties());
    }

    //暂时不确定
    public static class convertToTMediationParticipant implements Function1<Row, t_mediation_participant>, Serializable {
        @Override
        public t_mediation_participant apply (Row cas){
            t_mediation_participant tmediationcasepeople = new t_mediation_participant();
            //纠纷信息的主键id
            if(cas.getAs("id") !=null){
                tmediationcasepeople.setCase_id(Long.parseLong(cas.getAs("id").toString()));
            }

            //调解机构/调解员标识：1 调解机构、 2 调解员、3 协同调解员
            tmediationcasepeople.setFlag(1);
            //创建日期
            tmediationcasepeople.setCreate_time(DateUtil.date().toString());
            //更新日期
            tmediationcasepeople.setUpdate_time(DateUtil.date().toString());
            //纠纷机构id 766为肥东
            tmediationcasepeople.setOrg_id(766L);
            //案件流转参与者id 即能看到该纠纷数据的用户id 12310（叶秀）
            tmediationcasepeople.setUser_id(12310L);

            return tmediationcasepeople;
        }
    }
}
