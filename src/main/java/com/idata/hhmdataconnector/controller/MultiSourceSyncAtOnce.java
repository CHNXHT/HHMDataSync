package com.idata.hhmdataconnector.controller;

import com.idata.hhmdataconnector.RawDataSync;
import com.idata.hhmdataconnector.enums.DatabaseTable;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;

import static com.idata.hhmdataconnector.utils.tableUtil.deleteTableBeforeInsert;

/**
 * @description: 多源数据同步（一次）
 * @author: xiehaotian
 * @date: 2023/7/19 15:15
 */
@Component
@Api(tags = "原始数据同步",description = "原始所有数据一次同步")
@RestController
@RequestMapping("hhm/subsync")
public class MultiSourceSyncAtOnce {
    @ApiOperation(value="原始数据同步",notes="所有表数据一把梭哈")
    @GetMapping("/multiOnce")
    public void multiSourceSync(){
        for (DatabaseTable dataSource : DatabaseTable.values()) {
            String targetdatabase = dataSource.getTargetdatabasename();
            String database = dataSource.getDatabasename();
            System.out.println(database);
            String table = dataSource.getTablename();
            try {
                RawDataSync.MultisyncData(database,targetdatabase,table,"","","","");
                System.out.println(database+"的"+table+"表同步成功！");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
