package com.idata.hhmdataconnector.plugin.jmlt;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.enums.DataSource;
import com.idata.hhmdataconnector.model.hhm.t_mediation_case;
import com.idata.hhmdataconnector.utils.DateUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;
import java.io.Serializable;
import static com.idata.hhmdataconnector.ReadData.getRawDF;
import static com.idata.hhmdataconnector.utils.connectionUtil.hhm_mysqlProperties;
import static com.idata.hhmdataconnector.utils.tableUtil.deleteTableBeforeInsert;

public class vsjxxCaseSync {
    public static void main(String[] args) {
        String beginTime = "2018-01-01";
        String endtime = DateUtil.beginOfDay(DateUtil.yesterday()).toString("yyyy-MM-dd HH:mm:ss");
        String raw = "raw";
        dataSync(beginTime,endtime,raw);
    }
    public static void dataSync(String beginTime,String endTime, String raw) {
        SparkConf conf = new SparkConf();
        conf.set("spark.driver.cores","4");  //设置driver的CPU核数
//        conf.set("spark.driver.maxResultSize","2g"); //设置driver端结果存放的最大容量，这里设置成为2G，超过2G的数据,job就直接放弃，不运行了
        conf.set("spark.driver.memory","4g");  //driver给的内存大小
        conf.set("spark.executor.memory","8g");// 每个executor的内存

        SparkSession spark = SparkSession.builder()
                .appName("vsjxxCaseSync:"+beginTime)
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
        String tableName = "V_SJXX";//args[1];
        String targetTableName = "t_mediation_case";
        String beginTimeStr = DateUtil.parse(beginTime).toString("yyyyMMddHHmmss");
        String endTimeStr = DateUtil.parse(endTime).toString("yyyyMMddHHmmss");
        String other_raw = "";
        //获取来源表数据

        Dataset<Row> rawDF = getRawDF(spark, tableName, dataSourceName,"GXSJ",beginTimeStr,endTimeStr,raw);


        // jf_code
        Dataset<Row> jf_codeDF = getRawDF(spark, "t_manage_jmlt_jf_code", "HHM","GXSJ",beginTimeStr,endTimeStr,"")
                .select("name","code","tag_code").withColumnRenamed("code","code1")
                .distinct();

        Dataset<Row> jf_raw_join = rawDF.join(jf_codeDF, rawDF.col("AJXL").equalTo(jf_codeDF.col("code1")),"left");

        Dataset<Row> rowDataset = jf_raw_join.withColumn("SJRS", rawDF.col("SJRS").cast(DataTypes.LongType));


        //转化为目标表结构
        Dataset<t_mediation_case> tcDF = rowDataset
                .map(new ConvertToTMediationCase(), Encoders.bean(t_mediation_case.class));

        //数据入库前删除当前时间段表数据
//        deleteTableBeforeInsert(targetTableName, DataSource.HHM.getUrl(),DataSource.HHM.getUser(), DataSource.HHM.getPassword(), beginTimeStr,endTimeStr,"create_time","1");

        tcDF
                .distinct()
                .write()
                .mode(SaveMode.Append)
                .jdbc(DataSource.HHM.getUrl(), targetTableName, hhm_mysqlProperties());

        spark.close();
    }

    public static class ConvertToTMediationCase implements Function1<Row, t_mediation_case>, Serializable {
        @Override
        public t_mediation_case apply(Row vsjxx) {
            t_mediation_case tMediationCase = new t_mediation_case();
            if (vsjxx.getAs("ID")!= null){
                tMediationCase.setResource_id(vsjxx.getAs("ID").toString());
            }

            //创建时间
            if(vsjxx.getAs("CJSJ")!=null){
                tMediationCase.setCreate_time(DateUtils.strToTsSFM(vsjxx.getAs("CJSJ").toString()));
            }

            //修改时间
            if(vsjxx.getAs("GXSJ")!=null){
                tMediationCase.setUpdate_time(DateUtils.strToTsSFM(vsjxx.getAs("GXSJ").toString()));
            }

            //纠纷编号
            if(vsjxx.getAs("AJBH")!=null)
            tMediationCase.setCase_num(vsjxx.getAs("AJBH").toString());
            //纠纷描述
            tMediationCase.setCase_description(vsjxx.getAs("AJMS").toString());
            //纠纷诉求
//            tMediationCase.setRequest("-");
            //调解方式
            tMediationCase.setMethod(2);
            //证据材料描述
            tMediationCase.setEvidence_description("-");
            //纠纷类型
            if(vsjxx.getAs("AJXL") != null){
                tMediationCase.setCase_type(vsjxx.getAs("AJXL"));
            }
            // from feidong
            tMediationCase.setPlace_code("340000000000,340100000000,340122000000");
            //纠纷发生地
            if(vsjxx.getAs("FSDD")!=null){
                tMediationCase.setPlace_detail(vsjxx.getAs("FSDD").toString());
            }

            //纠纷发生日期
            if(vsjxx.getAs("FSSJ")!=null){
                tMediationCase.setOccurrence_time(DateUtils.strToTsSF(vsjxx.getAs("FSSJ").toString()));
            }

            //创建人ID
            tMediationCase.setCreate_user_id(10101L);
            //创建人姓名
            if(vsjxx.getAs("CJRXM")!=null){
                tMediationCase.setCreate_user_name(vsjxx.getAs("CJRXM").toString());
            }

            //文书状态
            tMediationCase.setDoc_status(0);
            //调解结果
            if (vsjxx.getAs("TJZT") != null) {
                tMediationCase.setResult(1);  //todo 检查确认
            }

            //纠纷状态 先处理办理状态 再处理调整状态
            if(vsjxx.getAs("BLZT") != null){
                String blzt = vsjxx.getAs("BLZT").toString();
                if(0 == Integer.parseInt(blzt)){
                    tMediationCase.setStatus(1);
                } else if(2 == Integer.parseInt(blzt)){
                    tMediationCase.setStatus(3);
                }else{
                    if(vsjxx.getAs("SJZT") != null){
                        String sjzt = vsjxx.getAs("SJZT").toString();
                        if(1 == Integer.parseInt(sjzt) || 2 == Integer.parseInt(sjzt) || 3 == Integer.parseInt(sjzt)){
                            tMediationCase.setStatus(2);
                        }else if(4 == Integer.parseInt(sjzt)){
                            tMediationCase.setStatus(7);
                        }else if(5 == Integer.parseInt(sjzt) || 6 == Integer.parseInt(sjzt) || 7 == Integer.parseInt(sjzt) || 8 == Integer.parseInt(sjzt)){
                            //原先4（结束），现在3（成功）
                            tMediationCase.setStatus(3);
                        }
                    }
                }
            }
            //纠纷来源 1为警民联调
            tMediationCase.setCase_source(1);

            if (vsjxx.getAs("tag_code")!= null){
                tMediationCase.setCase_type(vsjxx.getAs("tag_code").toString());
            }
            // 设置其他属性
            return tMediationCase;
        }
    }

}
