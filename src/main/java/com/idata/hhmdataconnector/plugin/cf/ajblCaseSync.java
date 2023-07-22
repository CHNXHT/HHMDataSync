package com.idata.hhmdataconnector.plugin.cf;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.enums.DataSource;
import com.idata.hhmdataconnector.model.hhm.t_mediation_case;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;
import java.io.Serializable;
import static com.idata.hhmdataconnector.ReadData.getRawDF;
import static com.idata.hhmdataconnector.utils.connectionUtil.hhm_mysqlProperties;
import static com.idata.hhmdataconnector.utils.tableUtil.deleteTableBeforeInsert;

/**
 * @description: some desc
 * @author: xiehaotian
 * @date: 2023/7/10 16:29
 */
public class ajblCaseSync {

    public static void dataSync(String beginTime,String endTime, String raw) {
        SparkConf conf = new SparkConf();
        conf.set("spark.driver.cores","4");  //设置driver的CPU核数
//        conf.set("spark.driver.maxResultSize","2g"); //设置driver端结果存放的最大容量，这里设置成为2G，超过2G的数据,job就直接放弃，不运行了
        conf.set("spark.driver.memory","4g");  //driver给的内存大小
        conf.set("spark.executor.memory","8g");// 每个executor的内存
        SparkSession spark = SparkSession.builder()
                .appName("ajblCaseSync:"+beginTime)
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
        String tableName = "T_SJKJ_RMTJ_AJBL";//args[1];
        String targetTableName = "t_mediation_case";
        String timeField = "SLRQ";

        String beginTimeStr = DateUtil.parse(beginTime).toString("yyyy-MM-dd HH:mm:ss");
        String endTimeStr = DateUtil.parse(endTime).toString("yyyy-MM-dd HH:mm:ss");
        Dataset<Row> rawDF = getRawDF(spark, tableName, dataSourceName, timeField, beginTimeStr,endTimeStr, raw);

        //获取来源表数据
        if(!beginTimeStr.equals("raw")){
            rawDF.where(rawDF.col("FSRQ").$greater(beginTime));
        }
//        Dataset<Row> rawDF = getRawDF(spark, tableName, dataSourceName).filter();
        Dataset<Row> rowDataset = rawDF.withColumn("SAJE", rawDF.col("SAJE").cast(DataTypes.LongType));
        //todo code SLDAW->T_SJKJ_RMTJ_TJWYH(XZDQ)->t_organization(province,city,county)
        Dataset<Row> TJWYHDF = getRawDF(spark, "T_SJKJ_RMTJ_TJWYH", dataSourceName, timeField, beginTimeStr,endTimeStr, "")
                .select("TWHMC","XZDQ")
                .distinct();

        Dataset<Row> ORGDF = getRawDF(spark, "t_organization", "HHM", timeField, beginTimeStr,endTimeStr, "")
                .select("province","city","county")
                .where("town = ''")
                .distinct();
//        ORGDF.withColumn("aa",ORGDF.col("county").substr(0,6)).show(5);

        Dataset<Row> raw_tjwyhDF = rawDF.join(TJWYHDF, rawDF.col("SLDW").equalTo(TJWYHDF.col("TWHMC")), "left");

        //定义数据源对象
        Dataset<Row> rowDF = raw_tjwyhDF.join(ORGDF, ORGDF.col("county").substr(0,6).equalTo(raw_tjwyhDF.col("XZDQ")),"left");

        //转化为目标表结构
        Dataset<t_mediation_case> tcDF = rowDF
                .map(new ConvertToTMediationCase(), Encoders.bean(t_mediation_case.class));
        tcDF.show(10);
        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(targetTableName, DataSource.HHM.getUrl(),DataSource.HHM.getUser(), DataSource.HHM.getPassword(), beginTimeStr,endTimeStr,"create_time","2");

        tcDF
                .distinct()
                .repartition(20)
                .write()
                .mode(SaveMode.Append)
                .jdbc(DataSource.HHM.getUrl(), targetTableName, hhm_mysqlProperties());

        spark.close();
    }

    public static class ConvertToTMediationCase implements Function1<Row, t_mediation_case>, Serializable {
        @Override
        public t_mediation_case apply(Row ajbl) {
            t_mediation_case tMediationCase = new t_mediation_case();
            if (ajbl.getAs("BH")!=null){
                tMediationCase.setResource_id(ajbl.getAs("BH").toString());
            }

            //创建时间
            if(ajbl.getAs("SLRQ")!=null){
                tMediationCase.setCreate_time(ajbl.getAs("SLRQ").toString());
            }
            //修改时间
//            tMediationCase.setUpdate_time(DateUtils.strToTsSFM(ajbl.getGXSJ()));
            //纠纷编号
            if(ajbl.getAs("AJBH")!=null){
                tMediationCase.setCase_num(ajbl.getAs("AJBH").toString());
            }

            //纠纷描述
            if(ajbl.getAs("JFJJ")!=null){
                tMediationCase.setCase_description(ajbl.getAs("JFJJ").toString());
            }

            //纠纷诉求
//            tMediationCase.setRequest("-");
            //调解方式(第三方)
            tMediationCase.setMethod(2);
            //证据材料描述
//            tMediationCase.setEvidence_description("-");
            //纠纷类型
            if (ajbl.getAs("JFLX") != null) {
                tMediationCase.setCase_type(ajbl.getAs("JFLX").toString());
            }
            //纠纷发生地 todo code
            if (ajbl.getAs("XZQH")!=null){
                tMediationCase.setPlace_detail(ajbl.getAs("XZQH").toString());
            }

            //todo code SLDAW->T_SJKJ_RMTJ_TJWYH(XZDQ)->t_organization(province,city,county)
            if ( ajbl.getAs("province")!=null && ajbl.getAs("city")!=null && ajbl.getAs("county")!=null){
                String placeCode= ajbl.getAs("province").toString()+","+ajbl.getAs("city").toString()+","+ajbl.getAs("county").toString();
                tMediationCase.setPlace_code(placeCode);
            }
            //todo 状态（成功3，结束4）
            if(ajbl.getAs("TJJG")!=null && ajbl.getAs("TJJG").toString().equals("成功")){
                tMediationCase.setStatus(3);
            }else if(ajbl.getAs("TJJG")!=null && ajbl.getAs("TJJG").toString().equals("不成功")){
                tMediationCase.setStatus(4);
            }

            //纠纷发生日期
            if(ajbl.getAs("FSRQ")!=null){
                tMediationCase.setOccurrence_time(ajbl.getAs("FSRQ").toString());
            }

            //创建人ID
            tMediationCase.setCreate_user_id(10101L);
            //创建人姓名
            if(ajbl.getAs("TJY")!=null){
                tMediationCase.setCreate_user_name(ajbl.getAs("TJY").toString());
            }

            //文书状态
            tMediationCase.setDoc_status(0);
            //调解结果
//            if (StringUtils.isBlank(ajbl.getTJZT())) {
//                tMediationCase.setResult(1);  //todo 检查确认
//            } else {
//                try {
//                    tMediationCase.setResult(Integer.parseInt(ajbl.getTJZT()));
//                } catch (NumberFormatException e) {
//                    // 处理转换异常，例如设定一个默认值或者抛出自定义异常
//                }
//            }

            //纠纷状态 先处理办理状态 再处理调整状态
//            String blzt = ajbl.getBLZT();
//            String sjzt = ajbl.getSJZT();
//            if(StringUtils.isAllBlank(blzt)){
//                if(0 == Integer.parseInt(blzt)){
//                    tMediationCase.setStatus(1);
//                } else if(2 == Integer.parseInt(blzt)){
//                    tMediationCase.setStatus(4);
//                }else{
//                    if(StringUtils.isAllBlank(sjzt)){
//                        if(1 == Integer.parseInt(sjzt) || 2 == Integer.parseInt(sjzt) || 3 == Integer.parseInt(sjzt)){
//                            tMediationCase.setStatus(2);
//                        }else if(4 == Integer.parseInt(sjzt)){
//                            tMediationCase.setStatus(7);
//                        }else if(5 == Integer.parseInt(sjzt) || 6 == Integer.parseInt(sjzt) || 7 == Integer.parseInt(sjzt) || 8 == Integer.parseInt(sjzt)){
//                            tMediationCase.setStatus(4);
//                        }
//                    }
//                }
//            }
            //纠纷来源 1为警民联调
            tMediationCase.setCase_source(2);
            // 设置其他属性
            return tMediationCase;
        }
    }

}
