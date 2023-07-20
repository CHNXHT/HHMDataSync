package com.idata.hhmdataconnector.controller;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.RawDataSync;
import com.idata.hhmdataconnector.enums.DataSource;
import com.idata.hhmdataconnector.enums.DatabaseTable;
import com.idata.hhmdataconnector.utils.Result;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.sql.SQLException;
import static com.idata.hhmdataconnector.utils.tableUtil.deleteTableBeforeInsert;

/**
 * @description: some desc
 * @author: xiehaotian
 * @date: 2023/7/19 15:58
 */
@Component
@Api(tags = "促法原始数据按天同步",description = "原始表数据按天同步")
@RestController
@RequestMapping("hhm/multibyday")
public class CFOriginSyncByDay {
    String url = DataSource.CF_ORIGIN.getUrl();
    String username = DataSource.CF_ORIGIN.getUser();
    String pasword = DataSource.CF_ORIGIN.getPassword();
    String raw = "raw";
    String begintime = DateUtil.beginOfDay(DateUtil.yesterday().toSqlDate()).toString("yyyy-MM-dd HH:mm:ss");//2023-07-17 00:00:00
    String endtime = DateUtil.today();//2023-07-18

    @ApiOperation(value="T_SJKJ_RMTJ_AJBL 原始数据同步",notes="T_SJKJ_RMTJ_AJBL 表数据按天同步")
    @GetMapping("/cft_sjkj_rmtj_ajbl")
    public Result cft_sjkj_rmtj_ajbl() throws SQLException {
        //警民联调 V_SJGXR
        String databasename = DatabaseTable.CFT_SJKJ_RMTJ_AJBL.getDatabasename();
        String targetdatabasename = DatabaseTable.CFT_SJKJ_RMTJ_AJBL.getTargetdatabasename();
        String tablename = DatabaseTable.CFT_SJKJ_RMTJ_AJBL.getTablename();
        String beginTimeStr = DateUtil.parse(begintime).toString("yyyy-MM-dd HH:mm:ss");
        String endTimeStr = DateUtil.parse(endtime).toString("yyyy-MM-dd HH:mm:ss");
        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(tablename, url,targetdatabasename, pasword, beginTimeStr,endTimeStr,"SLSJ","1");
        RawDataSync.MultisyncData(databasename,targetdatabasename,tablename,raw,"SLSJ",begintime,endtime);
        return Result.SUCCESS(begintime+"日："+"T_SJKJ_RMTJ_AJBL 数据同步成功");
    }

//    @ApiOperation(value="T_SJKJ_RMTJ_AJDSR 原始数据同步",notes="T_SJKJ_RMTJ_AJDSR 表数据按天同步")
//    @GetMapping("/t_sjkj_rmtj_ajdsr")
//    public Result t_sjkj_rmtj_ajdsr() throws SQLException {
//        //警民联调 V_SJLX
//        String databasename = DatabaseTable.JMLTV_SJLX.getDatabasename();
//        String tablename = DatabaseTable.JMLTV_SJLX.getTablename();
//        String beginTimeStr = DateUtil.parse(begintime).toString("yyyyMMddHHmmss");
//        String endTimeStr = DateUtil.parse(endtime).toString("yyyyMMddHHmmss");
//        //数据入库前删除当前时间段表数据
//        deleteTableBeforeInsert(tablename, url,databasename, pasword, beginTimeStr,endTimeStr,"GXSJ","1");
//        RawDataSync.syncData(databasename,tablename,raw,"GXSJ",begintime,endtime);
//        return Result.SUCCESS(begintime+"日："+"T_SJKJ_RMTJ_AJDSR 数据同步成功");
//    }

    @ApiOperation(value="T_SJKJ_RMTJ_DCJL 原始数据同步",notes="T_SJKJ_RMTJ_DCJL 表数据按天同步")
    @GetMapping("/cft_sjkj_rmtj_dcjl")
    public Result cft_sjkj_rmtj_dcjl() throws SQLException {
        //警民联调 V_SJLX
        String databasename = DatabaseTable.CFT_SJKJ_RMTJ_DCJL.getDatabasename();
        String tablename = DatabaseTable.CFT_SJKJ_RMTJ_DCJL.getTablename();
        String targetdatabasename = DatabaseTable.CFT_SJKJ_RMTJ_DCJL.getTargetdatabasename();
        String beginTimeStr = DateUtil.parse(begintime).toString("yyyy-MM-dd");
        String endTimeStr = DateUtil.parse(endtime).toString("yyyy-MM-dd");
        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(tablename, url,targetdatabasename, pasword, beginTimeStr,endTimeStr,"DCRQ","1");
        RawDataSync.MultisyncData(databasename,targetdatabasename,tablename,raw,"DCRQ",begintime,endtime);
        return Result.SUCCESS(begintime+"日："+"T_SJKJ_RMTJ_DCJL 数据同步成功");
    }

    @ApiOperation(value="T_SJKJ_RMTJ_TJGZS 原始数据同步",notes="T_SJKJ_RMTJ_TJGZS 表数据按天同步")
    @GetMapping("/cft_sjkj_rmtj_tjgzs")
    public Result cft_sjkj_rmtj_tjgzs() throws SQLException {
        //警民联调 V_SJLX
        String databasename = DatabaseTable.CFT_SJKJ_RMTJ_TJGZS.getDatabasename();
        String tablename = DatabaseTable.CFT_SJKJ_RMTJ_TJGZS.getTablename();
        String targetdatabasename = DatabaseTable.CFT_SJKJ_RMTJ_TJGZS.getTargetdatabasename();
        String beginTimeStr = DateUtil.parse(begintime).toString("yyyy-MM-dd");
        String endTimeStr = DateUtil.parse(endtime).toString("yyyy-MM-dd");
        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(tablename, url,targetdatabasename, pasword, beginTimeStr,endTimeStr,"TJGZSSLSJ","1");
        RawDataSync.MultisyncData(databasename,targetdatabasename,tablename,raw,"TJGZSSLSJ",begintime,endtime);
        return Result.SUCCESS(begintime+"日："+"T_SJKJ_RMTJ_TJGZS 数据同步成功");
    }

    @ApiOperation(value="T_SJKJ_RMTJ_TJJL 原始数据同步",notes="T_SJKJ_RMTJ_TJJL 表数据按天同步")
    @GetMapping("/cft_sjkj_rmtj_tjjl")
    public Result cft_sjkj_rmtj_tjjl() throws SQLException {
        //警民联调 V_SJLX
        String databasename = DatabaseTable.CFT_SJKJ_RMTJ_TJJL.getDatabasename();
        String tablename = DatabaseTable.CFT_SJKJ_RMTJ_TJJL.getTablename();
        String targetdatabasename = DatabaseTable.CFT_SJKJ_RMTJ_TJJL.getTargetdatabasename();
        String beginTimeStr = DateUtil.parse(begintime).toString("yyyy-MM-dd");
        String endTimeStr = DateUtil.parse(endtime).toString("yyyy-MM-dd");
        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(tablename, url,targetdatabasename, pasword, beginTimeStr,endTimeStr,"TJRQ","1");
        RawDataSync.MultisyncData(databasename,targetdatabasename,tablename,raw,"TJRQ",begintime,endtime);
        return Result.SUCCESS(begintime+"日："+"T_SJKJ_RMTJ_TJJL 数据同步成功");
    }

//    @ApiOperation(value="T_SJKJ_RMTJ_TJWYH 原始数据同步",notes="T_SJKJ_RMTJ_TJWYH 表数据按天同步")
//    @GetMapping("/cft_sjkj_rmtj_tjwyh")
//    public Result cft_sjkj_rmtj_tjwyh() throws SQLException {
//        //警民联调 V_SJLX
//        String databasename = DatabaseTable.CFT_SJKJ_RMTJ_TJWYH.getDatabasename();
//        String tablename = DatabaseTable.CFT_SJKJ_RMTJ_TJWYH.getTablename();
//        String targetdatabasename = DatabaseTable.CFT_SJKJ_RMTJ_TJWYH.getTargetdatabasename();
//        String beginTimeStr = DateUtil.parse(begintime).toString("yyyy-MM-dd HH:mm:ss");
//        String endTimeStr = DateUtil.parse(endtime).toString("yyyy-MM-dd HH:mm:ss");
//        //数据入库前删除当前时间段表数据
//        deleteTableBeforeInsert(tablename, url,targetdatabasename, pasword, beginTimeStr,endTimeStr,"XXCJRQ","1");
//        RawDataSync.MultisyncData(databasename,targetdatabasename,tablename,raw,"XXCJRQ",begintime,endtime);
//        return Result.SUCCESS(begintime+"日："+"T_SJKJ_RMTJ_TJWYH 数据同步成功");
//    }

    @ApiOperation(value="T_SJKJ_RMTJ_TJY 原始数据同步",notes="T_SJKJ_RMTJ_TJY 表数据按天同步")
    @GetMapping("/cft_sjkj_rmtj_tjy")
    public Result cft_sjkj_rmtj_tjy() throws SQLException {
        //警民联调 V_SJLX
        String databasename = DatabaseTable.CFT_SJKJ_RMTJ_TJY.getDatabasename();
        String tablename = DatabaseTable.CFT_SJKJ_RMTJ_TJY.getTablename();
        String targetdatabasename = DatabaseTable.CFT_SJKJ_RMTJ_TJY.getTargetdatabasename();
        String beginTimeStr = DateUtil.parse(begintime).toString("yyyy-MM-dd HH:mm:ss");
        String endTimeStr = DateUtil.parse(endtime).toString("yyyy-MM-dd HH:mm:ss");
        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(tablename, url,targetdatabasename, pasword, beginTimeStr,endTimeStr,"XXCJRQ","1");
        RawDataSync.MultisyncData(databasename,targetdatabasename,tablename,raw,"XXCJRQ",begintime,endtime);
        return Result.SUCCESS(begintime+"日："+"T_SJKJ_RMTJ_TJY 数据同步成功");
    }

}
