package com.idata.hhmdataconnector.enums;

/**
 * @description: some desc
 * @author: xiehaotian
 * @date: 2023/7/19 15:42
 */
public enum DatabaseTable {

    //警民联调
//    JMLTV_SJGXR("JMLT_ORIGIN","JMLT","V_SJGXR"),
//    JMLTV_SJLX("JMLT_ORIGIN","JMLT","V_SJLX"),
//    JMLTV_SJXX("JMLT_ORIGIN","JMLT","V_SJXX"),
//    JMLTV_SPJG("JMLT_ORIGIN","JMLT","V_SPJG"),
//    JMLTV_ZD("JMLT_ORIGIN","JMLT","V_ZD"),

    //促法
    CFT_SJKJ_RMTJ_AJBL("CF_ORIGIN","CF","T_SJKJ_RMTJ_AJBL"),
    CFT_SJKJ_RMTJ_AJDSR("CF_ORIGIN","CF","T_SJKJ_RMTJ_AJDSR"),
    CFT_SJKJ_RMTJ_DCJL("CF_ORIGIN","CF","T_SJKJ_RMTJ_DCJL"),
    CFT_SJKJ_RMTJ_TJGZS("CF_ORIGIN","CF","T_SJKJ_RMTJ_TJGZS"),
    CFT_SJKJ_RMTJ_TJJL("CF_ORIGIN","CF","T_SJKJ_RMTJ_TJJL"),
    CFT_SJKJ_RMTJ_TJWYH("CF_ORIGIN","CF","T_SJKJ_RMTJ_TJWYH"),
    CFT_SJKJ_RMTJ_TJY("CF_ORIGIN","CF","T_SJKJ_RMTJ_TJY"),
    CFT_SJKJ_SFXZ_JGXX("CF_ORIGIN","CF","T_SJKJ_SFXZ_JGXX");

    private String targetdatabasename;
    private String databasename;
    private String tablename;

    DatabaseTable(String targetdatabasename,String databasename, String tablename) {
        this.databasename = databasename;
        this.targetdatabasename = targetdatabasename;
        this.tablename = tablename;
    }

    public String getDatabasename() {
        return databasename;
    }

    public String getTargetdatabasename() {
        return targetdatabasename;
    }

    public void setTargetdatabasename(String targetdatabasename) {
        this.targetdatabasename = targetdatabasename;
    }

    public void setDatabasename(String databasename) {
        this.databasename = databasename;
    }

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }
}
