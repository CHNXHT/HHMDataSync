package com.idata.hhmdataconnector.controller;

import com.idata.hhmdataconnector.plugin.cf.ajblCaseSync;
import com.idata.hhmdataconnector.plugin.cf.ajblTJWYHParticpantSync;
import com.idata.hhmdataconnector.plugin.cf.ajdsrPeopleSync;
import com.idata.hhmdataconnector.plugin.cf.tjjlLogSync;
import com.idata.hhmdataconnector.plugin.jmlt.caseParticipantSync;
import com.idata.hhmdataconnector.plugin.jmlt.vsjgxrPeopleSync;
import com.idata.hhmdataconnector.plugin.jmlt.vsjxxCaseSync;
import com.idata.hhmdataconnector.plugin.jmlt.vspjgLogSync;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
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
public class JMLTSubModuleSync {
    //数据同步前先删除msyql数据时间内数据

    //case
    @ApiOperation(value = "警民联调-CASE",notes="警民联调Vsjxx表数据同步到case表")
    @GetMapping("/jmltCase")
    public void jmltCase(
            @ApiParam(name = "beginTime", value = "提取开始时间",required=true)String beginTime,
            @ApiParam(name = "endTime", value = "提取结束时间",required=true)String endTime,
            @ApiParam(name = "raw", value = "提取类型",required=true)String raw){
        vsjxxCaseSync.dataSync(beginTime,endTime,raw);
    }
    //people
    @ApiOperation("警民联调-PEOPLE")
    @GetMapping("/jmltPeople")
    public void jmltPeople(
            @ApiParam(name = "beginTime", value = "提取开始时间",required=true)String beginTime,
            @ApiParam(name = "endTime", value = "提取结束时间",required=true)String endTime,
            @ApiParam(name = "raw", value = "提取类型",required=true)String raw){
        vsjgxrPeopleSync.dataSync(beginTime,endTime,raw);
    }

    //people
    @ApiOperation("警民联调-Log")
    @GetMapping("/jmltLog")
    public void jmltLog(
            @ApiParam(name = "beginTime", value = "提取开始时间",required=true)String beginTime,
            @ApiParam(name = "endTime", value = "提取结束时间",required=true)String endTime,
            @ApiParam(name = "raw", value = "提取类型",required=true)String raw){
        vspjgLogSync.dataSync(beginTime,endTime,raw);
    }
    //people
    @ApiOperation("警民联调-Particpant")
    @GetMapping("/jmltParticpant")
    public void jmltParticpant(
            @ApiParam(name = "beginTime", value = "提取开始时间",required=true)String beginTime,
            @ApiParam(name = "endTime", value = "提取结束时间",required=true)String endTime,
            @ApiParam(name = "raw", value = "提取类型",required=true)String raw){
        caseParticipantSync.dataSync(beginTime,endTime,raw);
    }

}
