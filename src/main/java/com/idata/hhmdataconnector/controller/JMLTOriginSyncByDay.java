package com.idata.hhmdataconnector.controller;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.DatabaseTable;
import com.idata.hhmdataconnector.RawDataSync;
import com.idata.hhmdataconnector.enums.DataSource;
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
@Api(tags = "警民联调原始数据按天同步",description = "原始表数据按天同步")
@RestController
@RequestMapping("hhm/multibyday")
public class JMLTOriginSyncByDay {
    String url = DataSource.JMLT_ORIGIN.getUrl();
    String username = DataSource.JMLT_ORIGIN.getUser();
    String pasword = DataSource.JMLT_ORIGIN.getPassword();
    String raw = "raw";
    String begintime = DateUtil.beginOfDay(DateUtil.yesterday().toSqlDate()).toString("yyyy-MM-dd HH:mm:ss");//2023-07-17 00:00:00
    String endtime = DateUtil.today();//2023-07-18
    @ApiOperation(value="JMLTV_SJGXR原始数据同步",notes="jmltv_sjgxr表数据按天同步")
    @GetMapping("/jmltv_sjgxr")
    public Result jmltv_sjgxr() throws SQLException {
        //警民联调 V_SJGXR
        String databasename = DatabaseTable.JMLTV_SJGXR.getDatabasename();
        String tablename = DatabaseTable.JMLTV_SJGXR.getTablename();
        String beginTimeStr = DateUtil.parse(begintime).toString("yyyyMMddHHmmss");
        String endTimeStr = DateUtil.parse(endtime).toString("yyyyMMddHHmmss");
        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(tablename, url,databasename, pasword, beginTimeStr,endTimeStr,"GXSJ","1");
        RawDataSync.syncData(databasename,tablename,raw,"GXSJ",begintime,endtime);
        return Result.SUCCESS(begintime+"日："+"JMLTV_SJGXR 数据同步成功");
    }

    @ApiOperation(value="JMLTV_SJLX原始数据同步",notes="JMLTV_SJLX 表数据按天同步")
    @GetMapping("/jmltv_sjlx")
    public Result jmltv_sjlx() throws SQLException {
        //警民联调 V_SJLX
        String databasename = DatabaseTable.JMLTV_SJLX.getDatabasename();
        String tablename = DatabaseTable.JMLTV_SJLX.getTablename();
        String beginTimeStr = DateUtil.parse(begintime).toString("yyyyMMddHHmmss");
        String endTimeStr = DateUtil.parse(endtime).toString("yyyyMMddHHmmss");
        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(tablename, url,databasename, pasword, beginTimeStr,endTimeStr,"GXSJ","1");
        RawDataSync.syncData(databasename,tablename,raw,"GXSJ",begintime,endtime);
        return Result.SUCCESS(begintime+"日："+"JMLTV_SJLX 数据同步成功");
    }

    @ApiOperation(value="JMLTV_SJXX原始数据同步",notes="JMLTV_SJXX 表数据按天同步")
    @GetMapping("/jmltv_sjxx")
    public Result jmltv_sjxx() throws SQLException {
        //警民联调 V_SJLX
        String databasename = DatabaseTable.JMLTV_SJXX.getDatabasename();
        String tablename = DatabaseTable.JMLTV_SJXX.getTablename();
        String beginTimeStr = DateUtil.parse(begintime).toString("yyyyMMddHHmmss");
        String endTimeStr = DateUtil.parse(endtime).toString("yyyyMMddHHmmss");
        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(tablename, url,databasename, pasword, beginTimeStr,endTimeStr,"GXSJ","1");
        RawDataSync.syncData(databasename,tablename,raw,"GXSJ",begintime,endtime);
        return Result.SUCCESS(begintime+"日："+"V_SPJG 数据同步成功");
    }

    @ApiOperation(value="V_SPJG 原始数据同步",notes="V_SPJG 表数据按天同步")
    @GetMapping("/jmltv_spjg")
    public Result jmltv_spjg() throws SQLException {
        //警民联调 V_SJLX
        String databasename = DatabaseTable.JMLTV_SPJG.getDatabasename();
        String tablename = DatabaseTable.JMLTV_SPJG.getTablename();
        String beginTimeStr = DateUtil.parse(begintime).toString("yyyyMMddHHmmss");
        String endTimeStr = DateUtil.parse(endtime).toString("yyyyMMddHHmmss");
        //数据入库前删除当前时间段表数据
        deleteTableBeforeInsert(tablename, url,databasename, pasword, beginTimeStr,endTimeStr,"SPSJ","1");
        RawDataSync.syncData(databasename,tablename,raw,"SPSJ",begintime,endtime);
        return Result.SUCCESS(begintime+"日："+"V_SPJG 数据同步成功");
    }
}
