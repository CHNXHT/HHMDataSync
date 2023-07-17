package com.idata.hhmdataconnector.controller;
import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.plugin.cf.ajblCaseSync;
import com.idata.hhmdataconnector.plugin.cf.ajblTJWYHParticpantSync;
import com.idata.hhmdataconnector.plugin.cf.ajdsrPeopleSync;
import com.idata.hhmdataconnector.plugin.cf.tjjlLogSync;
import com.idata.hhmdataconnector.plugin.jmlt.caseParticipantSync;
import com.idata.hhmdataconnector.plugin.jmlt.vsjgxrPeopleSync;
import com.idata.hhmdataconnector.plugin.jmlt.vsjxxCaseSync;
import com.idata.hhmdataconnector.plugin.jmlt.vspjgLogSync;
import com.idata.hhmdataconnector.utils.Result;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
/**
 * @description: hhm原始数据同步（只同步一次）
 * @author: xiehaotian
 * @date: 2023/7/17 10:00
 */
@RestController
@RequestMapping("hhm/sync")
@Component
public class rawSyncController {

    @GetMapping("/raw")
//    @Scheduled(cron="0 0 1 * * ?")
    public Result rawHHMSync(){
        String raw = "raw";
        String begintime = DateUtil.beginOfDay(DateUtil.lastMonth()).toString("yyyy-MM-dd HH:mm:ss");
        String endtime = DateUtil.beginOfDay(DateUtil.lastMonth()).toString("yyyy-MM-dd HH:mm:ss");
        //hhm case表sync
        ajblCaseSync.dataSync(begintime,endtime,raw);
        vsjxxCaseSync.dataSync(begintime,endtime,raw);

        //hhm people表sync
        ajdsrPeopleSync.dataSync(begintime,endtime,raw);
        vsjgxrPeopleSync.dataSync(begintime,endtime,raw);

        //hhm log表sync
        tjjlLogSync.dataSync(begintime,endtime,raw);
        vspjgLogSync.dataSync(begintime,endtime,raw);

        //hhm Participant表sync
        ajblTJWYHParticpantSync.dataSync(begintime,endtime,raw);
        caseParticipantSync.dataSync(begintime,endtime,raw);

        return Result.SUCCESS(begintime+"日："+"hhm原始数据同步成功");
    }
}
