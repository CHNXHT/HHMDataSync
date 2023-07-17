package com.idata.hhmdataconnector.controller;

import com.idata.hhmdataconnector.plugin.cf.ajblCaseSync;
import com.idata.hhmdataconnector.plugin.cf.ajblTJWYHParticpantSync;
import com.idata.hhmdataconnector.plugin.cf.ajdsrPeopleSync;
import com.idata.hhmdataconnector.plugin.cf.tjjlLogSync;
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
    @ApiOperation(value="促法-CASE")
    @GetMapping("/cfCase")
    public void cfCase(
            @ApiParam(name = "beginTime", value = "提取开始时间",required=true)String beginTime,
            @ApiParam(name = "endTime", value = "提取结束时间",required=true)String endTime,
            @ApiParam(name = "raw", value = "提取类型",required=true)String raw){
        ajblCaseSync.dataSync(beginTime,endTime,raw);
    }
    //people
    @ApiOperation("促法-PEOPLE")
    @GetMapping("/cfPeople")
    public void cfPeople(
            @ApiParam(name = "beginTime", value = "提取开始时间",required=true)String beginTime,
            @ApiParam(name = "endTime", value = "提取结束时间",required=true)String endTime,
            @ApiParam(name = "raw", value = "提取类型",required=true)String raw){
        ajdsrPeopleSync.dataSync(beginTime,endTime,raw);
    }

    //people
    @ApiOperation("促法-Log")
    @GetMapping("/cfLog")
    public void cfLog(
            @ApiParam(name = "beginTime", value = "提取开始时间",required=true)String beginTime,
            @ApiParam(name = "endTime", value = "提取结束时间",required=true)String endTime,
            @ApiParam(name = "raw", value = "提取类型",required=true)String raw){
        tjjlLogSync.dataSync(beginTime,endTime,raw);
    }
    //people
    @ApiOperation("促法-Particpant")
    @GetMapping("/cfParticpant")
    public void cfParticpant(
            @ApiParam(name = "beginTime", value = "提取开始时间",required=true)String beginTime,
            @ApiParam(name = "endTime", value = "提取结束时间",required=true)String endTime,
            @ApiParam(name = "raw", value = "提取类型",required=true)String raw){
        ajblTJWYHParticpantSync.dataSync(beginTime,endTime,raw);
    }

}
