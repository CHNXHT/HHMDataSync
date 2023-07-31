package com.idata.hhmdataconnector.plugin.cf;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.enums.DataSource;
import com.idata.hhmdataconnector.model.cf.T_SJKJ_RMTJ_AJDSR;
import com.idata.hhmdataconnector.model.hhm.t_mediation_case_people;
import com.idata.hhmdataconnector.model.jmlt.V_ZD;
import com.idata.hhmdataconnector.utils.idCardUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import scala.Function1;

import java.io.Serializable;

import static com.idata.hhmdataconnector.ReadData.getRawDF;
import static com.idata.hhmdataconnector.utils.EncryUtils.encrypt;
import static com.idata.hhmdataconnector.utils.connectionUtil.hhm_mysqlProperties;
import static com.idata.hhmdataconnector.utils.tableUtil.deleteTableBeforeInsert;

/**
 * todo 数据原来不全，目前无法使用
 * @description: some desc
 * @author: xiehaotian
 * @date: 2023/7/10 18:23
 */
public class ajdsrPeopleSync {
    public static void main(String[] args) {
//        dataSync("","");
    }
    public static void dataSync(String beginTime,String endTime, String raw) {
        SparkConf conf = new SparkConf();
        conf.set("spark.driver.cores","4");  //设置driver的CPU核数
//        conf.set("spark.driver.maxResultSize","2g"); //设置driver端结果存放的最大容量，这里设置成为2G，超过2G的数据,job就直接放弃，不运行了
        conf.set("spark.driver.memory","4g");  //driver给的内存大小
        conf.set("spark.executor.memory","8g");// 每个executor的内存
        SparkSession spark = SparkSession.builder()
                .appName("ajdsrPeopleSync:"+beginTime)
                .config(conf)
                .master("local[20]")
                .getOrCreate();
        /*
          dataSourceName包括如下
          1、JMLT
          2、CF
          3、HHM
         */
        String dataSourceName = "CF";//args[0];
        String tableName = "T_SJKJ_RMTJ_AJDSR";//args[1];
        String targetTableName = "t_mediation_case_people";
        String beginTimeStr = DateUtil.parse(beginTime).toString("yyyy-MM-dd HH:mm:ss");
        String endTimeStr = DateUtil.parse(endTime).toString("yyyy-MM-dd HH:mm:ss");
        //获取来源表数据

        Dataset<Row> rawDF = getRawDF(spark, tableName, dataSourceName,"",beginTimeStr,endTimeStr,"");
        Dataset<Row> rowDataset = rawDF;
        //关联case表获取id
        Dataset<Row> caseDF = getRawDF(spark, "t_mediation_case", "HHM","create_time",beginTimeStr,endTimeStr,"raw")
                .where("case_source = '2'")
                .select("id","case_num");

        //定义数据源对象
        Dataset<T_SJKJ_RMTJ_AJDSR> rowDF = rowDataset.as(Encoders.bean(T_SJKJ_RMTJ_AJDSR.class));
        //需要和case ，ZD表join
        Dataset<V_ZD> vzdDF = getRawDF(spark, "V_ZD", "JMLT","","",endTimeStr,"")
                .as(Encoders.bean(V_ZD.class));

        Dataset<Row> joinDF = caseDF
                .join(rowDF, rowDF.col("AJBH").equalTo(caseDF.col("case_num")))
                .join(vzdDF, rowDF.col("MZ").equalTo(vzdDF.col("DM")), "left").where(vzdDF.col("LXJP").equalTo("MZ"));

        //转化为目标表结构
        Dataset<t_mediation_case_people> tcDF = joinDF
                .map(new convertToTMediationPeople(), Encoders.bean(t_mediation_case_people.class));

        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(targetTableName, DataSource.HHM.getUrl(),DataSource.HHM.getUser(), DataSource.HHM.getPassword(), beginTimeStr,endTimeStr,"create_time","1");

//        tcDF.show(10);
        //执行前先清空case_source=2的数据
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
            //创建时间(使用当前时间)
//            if (vsjgxr.getAs("CJSJ")!=null){
            tmediationcasepeople.setCreate_time(DateUtil.date().toString());
//            }

            //更新时间(使用当前时间)
//            if (vsjgxr.getAs("GXSJ")!=null) {
            tmediationcasepeople.setUpdate_time(DateUtil.date().toString());
//            }
            //参与人类型：1 申请人、2 被申请人
            if (vsjgxr.getAs("DSRDW")!=null) {
                tmediationcasepeople.setClass(vsjgxr.getAs("DSRDW").toString().equals("申请人") ? 1 : 2);
            }else {
                tmediationcasepeople.setClass(0);
            }
            //申请人、被申请人类型：1 自然人、2 法人、3 非法人组织
            if(vsjgxr.getAs("DSRLX")!=null){
                tmediationcasepeople.setType(Integer.parseInt(vsjgxr.getAs("DSRLX").toString()));
            }
            //申请人姓名/企业名称
            if (vsjgxr.getAs("XMMC")!=null) {
                tmediationcasepeople.setName(vsjgxr.getAs("XMMC").toString());
            }
            //自然人证件类型：1 居民身份证、2 护照
            if (vsjgxr.getAs("DSRZJLX")!=null) {
                tmediationcasepeople.setIdentity_type(vsjgxr.getAs("DSRZJLX").toString().equals("居民身份证") ? 1 : 2);
            }else {
                tmediationcasepeople.setIdentity_type(0);
            }
            //自然人证件号码
            if (vsjgxr.getAs("DSRZJHM")!=null) {
                String dsrzjhm = encrypt(vsjgxr.getAs("DSRZJHM").toString(), "MQ(KviV@#+$f@cPTlo*g5Wf4M6j1(PGH");
                tmediationcasepeople.setIdentity_num(dsrzjhm);
            }
//            //自然人性别：1 男性、2 女性
//            if (vsjgxr.getAs("GXRXB")!=null) {
//                tmediationcasepeople.setGender(vsjgxr.getAs("GXRXB").toString().equals("1") ? 1 : 2);
//            }else{
//                tmediationcasepeople.setGender(0);
//            }
            //联系电话
            if (vsjgxr.getAs("DSRLXDH")!=null) {
                String dsrlxdh = encrypt(vsjgxr.getAs("DSRLXDH").toString(), "lWTF^_FQS9PKMX!LrUfKkj5WkUUv9Sxs");
                tmediationcasepeople.setPhone(dsrlxdh);
            }
            //地址-国家行政区代码
//            if (vsjgxr.getAs("GXSJ")!=null) {
//                tmediationcasepeople.setPlace_code("-");
//            }
            //地址-详细地址
            if (vsjgxr.getAs("DSRDZ")!=null) {
                tmediationcasepeople.setPlace_detail(vsjgxr.getAs("DSRDZ").toString());
            }
            //社会信用码
//        tmediationcasepeople.setCredit_code("-");
            //法定代表人
//        tmediationcasepeople.setCreditCode("-");
            //申请人/被申请人年龄
            if (vsjgxr.getAs("NL")!=null) {
                tmediationcasepeople.setAge(String.valueOf(idCardUtils.getAgeByIDNumber(vsjgxr.getAs("NL").toString())));
            }
            //民族(直接汉字显示)
//        String mc =vsjgxr.getAs("DM").toString();
            if (vsjgxr.getAs("DM")!=null) {
                tmediationcasepeople.setNation(vsjgxr.getAs("DM").toString());
            }
//            //职位/职务(直接汉字显示)
//            if (vsjgxr.getAs("ZY")!=null) {
//                tmediationcasepeople.setPosition(vsjgxr.getAs("ZY").toString());
//            }
            //纠纷案件id
            if (vsjgxr.getAs("id")!=null) {
                tmediationcasepeople.setCase_id(Long.parseLong(vsjgxr.getAs("id").toString()));
            }
            // 设置其他属性
            return tmediationcasepeople;
        }
    }
}
