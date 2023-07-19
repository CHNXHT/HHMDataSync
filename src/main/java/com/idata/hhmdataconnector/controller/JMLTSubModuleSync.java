package com.idata.hhmdataconnector.controller;

import com.idata.hhmdataconnector.plugin.cf.ajblCaseSync;
import com.idata.hhmdataconnector.plugin.cf.ajblTJWYHParticpantSync;
import com.idata.hhmdataconnector.plugin.cf.ajdsrPeopleSync;
import com.idata.hhmdataconnector.plugin.cf.tjjlLogSync;
import com.idata.hhmdataconnector.plugin.jmlt.caseParticipantSync;
import com.idata.hhmdataconnector.plugin.jmlt.vsjgxrPeopleSync;
import com.idata.hhmdataconnector.plugin.jmlt.vsjxxCaseSync;
import com.idata.hhmdataconnector.plugin.jmlt.vspjgLogSync;
import com.idata.hhmdataconnector.utils.Result;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description: 各数据源表数据同步接口
 * @author: xiehaotian
 * @date: 2023/7/17 11:04
 */
@Api(tags = "警民联调",description = "提供警民联调数据同步接口")
@RestController
@RequestMapping("hhm/subsync")
@Component
public class JMLTSubModuleSync {
    //数据同步前先删除msyql数据时间内数据

    //case
    @ApiOperation(value = "警民联调-CASE",notes="警民联调 V_SJXX 表数据同步到 t_mediation_case 表")
    @GetMapping("/jmltCase")
    public Result jmltCase(
            @ApiParam(value = "提取开始时间",required=true)String beginTime,
            @ApiParam(value = "提取结束时间",required=true)String endTime,
            @ApiParam(value = "提取类型",required=true,defaultValue = "raw")String raw){
        vsjxxCaseSync.dataSync(beginTime,endTime,raw);
        return Result.SUCCESS(beginTime+"日："+"警民联调-CASE数据同步成功");
    }
    //people
    @ApiOperation(value = "警民联调-PEOPLE",notes="警民联调 V_SJGXR 表数据同步到 t_mediation_case_people 表")
    @GetMapping("/jmltPeople")
    public Result jmltPeople(
            @ApiParam(value = "提取开始时间",required=true)String beginTime,
            @ApiParam(value = "提取结束时间",required=true)String endTime,
            @ApiParam(value = "提取类型",required=true,defaultValue = "raw")String raw){
        vsjgxrPeopleSync.dataSync(beginTime,endTime,raw);
        return Result.SUCCESS(beginTime+"日："+"警民联调-PEOPLE数据同步成功");
    }

    //people
    @ApiOperation(value = "警民联调-Log",notes="警民联调 V_SPJG 表数据同步到 t_mediation_case_log 表")
    @GetMapping("/jmltLog")
    public Result jmltLog(
            @ApiParam(value = "提取开始时间",required=true)String beginTime,
            @ApiParam(value = "提取结束时间",required=true)String endTime,
            @ApiParam(value = "提取类型",required=true,defaultValue = "raw")String raw){
        vspjgLogSync.dataSync(beginTime,endTime,raw);
        return Result.SUCCESS(beginTime+"日："+"警民联调-Log数据同步成功");
    }
    //people
    @ApiOperation(value = "警民联调-Particpant",notes="警民联调 t_mediation_case 表数据同步到 t_mediation_participant 表")
    @GetMapping("/jmltParticpant")
    public Result jmltParticpant(
            @ApiParam(value = "提取开始时间",required=true)String beginTime,
            @ApiParam(value = "提取结束时间",required=true)String endTime,
            @ApiParam(value = "提取类型",required=true,defaultValue = "raw")String raw){
        caseParticipantSync.dataSync(beginTime,endTime,raw);
        return Result.SUCCESS(beginTime+"日："+"警民联调-Particpant数据同步成功");
    }

}
