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

/**
 * @description: some desc
 * @author: xiehaotian
 * @date: 2023/7/17 10:01
 */
@Component
public class TPlusOneSyncController {

    @Scheduled(cron="0 0 1 * * ?")
//    @Scheduled(cron="0 10 15 * * ?")
    public Result HHMSyncByDay(){
        String raw = "raw";
        String begintime = DateUtil.beginOfDay(DateUtil.yesterday().toSqlDate()).toString("yyyy-MM-dd HH:mm:ss");//2023-07-17 00:00:00
        String endtime = DateUtil.today();//2023-07-18
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

    public static void main(String[] args) {
        String begintime = DateUtil.beginOfDay(DateUtil.yesterday().toSqlDate()).toString("yyyy-MM-dd HH:mm:ss");
        String endtime = DateUtil.endOfDay(DateUtil.yesterday().toSqlDate()).toString("yyyy-MM-dd HH:mm:ss");
        System.out.println(begintime+"============"+endtime+"=="+DateUtil.parse(DateUtil.today()).toString("yyyy-MM-dd HH:mm:ss"));
    }

}
