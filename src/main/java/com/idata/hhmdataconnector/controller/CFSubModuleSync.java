package com.idata.hhmdataconnector.controller;

import com.idata.hhmdataconnector.plugin.cf.ajblCaseSync;
import com.idata.hhmdataconnector.plugin.cf.ajblTJWYHParticpantSync;
import com.idata.hhmdataconnector.plugin.cf.ajdsrPeopleSync;
import com.idata.hhmdataconnector.plugin.cf.tjjlLogSync;
import com.idata.hhmdataconnector.utils.Result;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description: 各数据源表数据同步接口
 * @author: xiehaotian
 * @date: 2023/7/17 11:04
 */
@Api(tags = "促法",description = "提供促法数据同步接口")
@RestController
@RequestMapping("hhm/subsync")
public class CFSubModuleSync {
    //数据同步前先删除msyql数据时间内数据

    //case
    @ApiOperation(value="促法-CASE",notes="促法 T_SJKJ_RMTJ_AJBL 表数据同步到 t_mediation_case 表")
    @GetMapping("/cfCase")
    public Result cfCase(
            @ApiParam(value = "提取开始时间",required=true)String beginTime,
            @ApiParam(value = "提取结束时间",required=true)String endTime,
            @ApiParam(value = "提取类型",required=true,defaultValue = "raw")String raw){
        ajblCaseSync.dataSync(beginTime,endTime,raw);
        return Result.SUCCESS(beginTime+"日："+"促法-CASE数据同步成功");
    }
    //people
    @ApiOperation(value = "促法-PEOPLE",notes="促法 T_SJKJ_RMTJ_AJDSR 表数据同步到 t_mediation_case_people 表")
    @GetMapping("/cfPeople")
    public Result cfPeople(
            @ApiParam(value = "提取开始时间",required=true)String beginTime,
            @ApiParam(value = "提取结束时间",required=true)String endTime,
            @ApiParam(value = "提取类型",required=true,defaultValue = "raw")String raw){
        ajdsrPeopleSync.dataSync(beginTime,endTime,raw);
        return Result.SUCCESS(beginTime+"日："+"促法-CASE数据同步成功");
    }

    //people
    @ApiOperation(value = "促法-Log",notes="促法 T_SJKJ_RMTJ_TJJL 表数据同步到 t_mediation_case_log 表")
    @GetMapping("/cfLog")
    public Result cfLog(
            @ApiParam(value = "提取开始时间",required=true)String beginTime,
            @ApiParam(value = "提取结束时间",required=true)String endTime,
            @ApiParam(value = "提取类型",required=true,defaultValue = "raw")String raw){
        tjjlLogSync.dataSync(beginTime,endTime,raw);
        return Result.SUCCESS(beginTime+"日："+"促法-LOG数据同步成功");
    }
    //people
    @ApiOperation(value = "促法-Particpant",notes="促法 T_SJKJ_RMTJ_TJWYH 表数据同步到 t_mediation_participant 表")
    @GetMapping("/cfParticpant")
    public Result cfParticpant(
            @ApiParam(value = "提取开始时间",required=true)String beginTime,
            @ApiParam(value = "提取结束时间",required=true)String endTime,
            @ApiParam(value = "提取类型",required=true,defaultValue = "raw")String raw){
        ajblTJWYHParticpantSync.dataSync(beginTime,endTime,raw);
        return Result.SUCCESS(beginTime+"日："+"促法-Particpant数据同步成功");
    }

}
