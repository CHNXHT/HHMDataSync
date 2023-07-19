package com.idata.hhmdataconnector.plugin.jmlt;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.DataSource;
import com.idata.hhmdataconnector.model.hhm.t_mediation_case_people;
import com.idata.hhmdataconnector.model.jmlt.V_SJGXR;
import com.idata.hhmdataconnector.model.jmlt.V_ZD;
import com.idata.hhmdataconnector.utils.DateUtils;
import com.idata.hhmdataconnector.utils.idCardUtils;
import org.apache.spark.sql.*;
import scala.Function1;

import java.io.Serializable;

import static com.idata.hhmdataconnector.ReadData.getRawDF;
import static com.idata.hhmdataconnector.utils.connectionUtil.hhm_mysqlProperties;
import static com.idata.hhmdataconnector.utils.tableUtil.deleteTableBeforeInsert;

/**
 * @description: some desc
 * @author: xiehaotian
 * @date: 2023/7/10 11:07
 */
public class vsjgxrPeopleSync {
    public static void main(String[] args) {
//        String begintime = DateUtil.beginOfDay(DateUtil.lastMonth()).toString("yyyyMMddHHmmss");
//        String raw = "oneday";
//        dataSync(begintime, raw);
    }
    public static void dataSync(String beginTime,String endTime, String raw) {
        SparkSession spark = SparkSession.builder()
                .appName("vsjgxrPeopleSync:"+beginTime)
                .master("local[20]")
                .getOrCreate();
        /*
          dataSourceName包括如下
          1、JMLT
          2、CF
          3、HHM
         */
        String dataSourceName = "JMLT";//args[0];
        String tableName = "V_SJGXR";//args[1];
        String targetTableName = "t_mediation_case_people_test";
        String beginTimeStr = DateUtil.parse(beginTime).toString("yyyyMMddHHmmss");
        String endTimeStr = DateUtil.parse(endTime).toString("yyyyMMddHHmmss");
        String other_raw = "";
        //获取来源表数据
        Dataset<Row> rawDF = getRawDF(spark, tableName, dataSourceName,"GXSJ",beginTimeStr,endTimeStr,raw);
        Dataset<Row> rowDataset = rawDF;

        //定义数据源对象
        Dataset<V_SJGXR> rowDF = rowDataset.as(Encoders.bean(V_SJGXR.class));
        //需要和case ，ZD表join
        //关联case表获取id
        Dataset<Row> caseDF = getRawDF(spark, "t_mediation_case_test", "HHM","","","",other_raw)
                .select("id","resource_id");
        Dataset<V_ZD> vzdDF = getRawDF(spark, "V_ZD", dataSourceName,"","","",other_raw)
                .as(Encoders.bean(V_ZD.class));

        Dataset<Row> joinDF = rowDF
                .join(caseDF, rowDF.col("AJID").equalTo(caseDF.col("resource_id")), "left")
                .join(vzdDF, rowDF.col("MZ").equalTo(vzdDF.col("DM")), "left").where(vzdDF.col("LXJP").equalTo("MZ"));
        joinDF.show(10);
        //转化为目标表结构
        Dataset<t_mediation_case_people> tcDF = joinDF
                .map(new convertToTMediationPeople(), Encoders.bean(t_mediation_case_people.class));

        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(targetTableName, DataSource.HHM.getUrl(),DataSource.HHM.getUser(), DataSource.HHM.getPassword(), beginTimeStr,endTimeStr,"create_time","1");

        tcDF.show(10);
        tcDF
                .repartition(20)
                .write()
                .mode(SaveMode.Append)
                .jdbc(DataSource.HHM.getUrl(), targetTableName, hhm_mysqlProperties());
        spark.close();
    }

    public static class convertToTMediationPeople implements Function1<Row, t_mediation_case_people>, Serializable {
        @Override
        public t_mediation_case_people apply (Row vsjgxr){
            t_mediation_case_people tmediationcasepeople = new t_mediation_case_people();
            //创建时间
            if (vsjgxr.getAs("CJSJ")!=null){
                tmediationcasepeople.setCreate_time(DateUtils.strToTsSFM(vsjgxr.getAs("CJSJ").toString()));
            }
            //更新时间
            if (vsjgxr.getAs("GXSJ")!=null) {
                tmediationcasepeople.setUpdate_time(DateUtil.now());
            }
            //参与人类型：1 申请人、2 被申请人
            if (vsjgxr.getAs("RYLB")!=null) {
                tmediationcasepeople.setClass(vsjgxr.getAs("RYLB") != null ? (vsjgxr.getAs("RYLB").toString().equals("申请人") ? 1 : 2) : 0);
            }
            //申请人、被申请人类型：1 自然人、2 法人、3 非法人组织
            tmediationcasepeople.setType(0);
            //申请人姓名/企业名称
            if (vsjgxr.getAs("GXRXM")!=null) {
                tmediationcasepeople.setName(vsjgxr.getAs("GXRXM").toString());
            }
            //自然人证件类型：1 居民身份证、2 护照
            if (vsjgxr.getAs("GXRZJLX")!=null) {
                tmediationcasepeople.setIdentity_type(vsjgxr.getAs("GXRZJLX").toString().equals("居民身份证") ? 1 : 2);
            }else {
                tmediationcasepeople.setIdentity_type(0);
            }
            //自然人证件号码
            if (vsjgxr.getAs("GXRZJH")!=null) {
                tmediationcasepeople.setIdentity_num(vsjgxr.getAs("GXRZJH").toString());
            }
            //自然人性别：1 男性、2 女性
            if (vsjgxr.getAs("GXRXB")!=null) {
                tmediationcasepeople.setGender(vsjgxr.getAs("GXRXB").toString().equals("1") ? 1 : 2);
            }else{
                tmediationcasepeople.setGender(0);
            }
            //联系电话
            if (vsjgxr.getAs("GXRLXFS")!=null) {
                tmediationcasepeople.setPhone(vsjgxr.getAs("GXRLXFS").toString());
            }
            //地址-国家行政区代码
//            if (vsjgxr.getAs("GXSJ")!=null) {
//                tmediationcasepeople.setPlace_code("-");
//            }
            //地址-详细地址
            if (vsjgxr.getAs("GXRDZ")!=null) {
                tmediationcasepeople.setPlace_detail(vsjgxr.getAs("GXRDZ").toString());
            }
            //社会信用码
//        tmediationcasepeople.setCredit_code("-");
            //法定代表人
//        tmediationcasepeople.setCreditCode("-");
            //申请人/被申请人年龄
            if (vsjgxr.getAs("GXRZJH")!=null) {
                tmediationcasepeople.setAge(String.valueOf(idCardUtils.getAgeByIDNumber(vsjgxr.getAs("GXRZJH").toString())));
            }
            //民族(直接汉字显示)
//        String mc =vsjgxr.getAs("DM").toString();
            if (vsjgxr.getAs("DM")!=null) {
                tmediationcasepeople.setNation(vsjgxr.getAs("DM").toString());
            }
            //职位/职务(直接汉字显示)
            if (vsjgxr.getAs("ZY")!=null) {
                tmediationcasepeople.setPosition(vsjgxr.getAs("ZY").toString());
            }
            //纠纷案件id
            if (vsjgxr.getAs("id")!=null) {
                tmediationcasepeople.setCase_id(Long.parseLong(vsjgxr.getAs("id").toString()));
            }
            // 设置其他属性
            return tmediationcasepeople;
        }
    }
}