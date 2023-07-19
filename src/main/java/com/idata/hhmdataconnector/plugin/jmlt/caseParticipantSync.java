package com.idata.hhmdataconnector.plugin.jmlt;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.enums.DataSource;
import com.idata.hhmdataconnector.model.hhm.t_mediation_participant;
import org.apache.spark.sql.*;
import scala.Function1;
import java.io.Serializable;
import static com.idata.hhmdataconnector.ReadData.getRawDF;
import static com.idata.hhmdataconnector.utils.connectionUtil.hhm_mysqlProperties;

/**
 * @description: some desc
 * @author: xiehaotian
 * @date: 2023/7/10 14:29
 */
public class caseParticipantSync {
    public static void main(String[] args) {
//        String begintime = DateUtil.beginOfDay(DateUtil.lastMonth()).toString("yyyy-MM-dd HH:mm:ss");
//        String raw = "oneday";
//        dataSync(begintime, raw);
    }
    public static void dataSync(String beginTime,String endTime, String raw) {
        SparkSession spark = SparkSession.builder()
                .appName("caseParticipantSync:"+beginTime)
                .master("local[20]")
                .getOrCreate();
        /*
          dataSourceName包括如下
          1、JMLT
          2、CF
          3、HHM
         */
        String dataSourceName = "HHM";//args[0];
        String tableName = "t_mediation_case_test";//args[1];
        String targetTableName = "t_mediation_participant_test";
        String beginTimeStr = DateUtil.parse(beginTime).toString("yyyy-MM-dd HH:mm:ss");
        String endTimeStr = DateUtil.parse(beginTime).toString("yyyy-MM-dd HH:mm:ss");
        //获取来源表数据
        Dataset<Row> rawDF = getRawDF(spark, tableName, dataSourceName,"create_time",beginTimeStr,endTimeStr,"oneday");

        //转化为目标表结构
        Dataset<t_mediation_participant> tcDF = rawDF
//                .where(rawDF.col("create_time").$greater(beginTime))
                .map(new convertToTMediationParticipant(), Encoders.bean(t_mediation_participant.class));

        //数据入库前删除当前时间段表数据
//        deleteTableBeforeInsert(targetTableName, DataSource.HHM.getUrl(),DataSource.HHM.getUser(), DataSource.HHM.getPassword(), beginTimeStr,endTimeStr,"update_time","2");
        tcDF.distinct().show(10);
        tcDF
                .distinct()
                .repartition(20)
                .write()
                .mode(SaveMode.Append)
                .jdbc(DataSource.HHM.getUrl(), targetTableName, hhm_mysqlProperties());
        spark.close();
    }

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
            tmediationcasepeople.setUpdate_time(DateUtil.now());
            //纠纷机构id 766为肥东
            tmediationcasepeople.setOrg_id(766L);
            //案件流转参与者id 即能看到该纠纷数据的用户id 12310（叶秀）
            tmediationcasepeople.setUser_id(12310L);

            return tmediationcasepeople;
        }
    }
}
