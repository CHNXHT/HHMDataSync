package com.idata.hhmdataconnector.enums;

public enum DataSource {

    //警民联调
    JMLT("jdbc:oracle:thin:@10.27.13.140:1521:zjkdb", "U_MT", "WYD5QzdzyyXG"),
    //促法
    CF("jdbc:oracle:thin:@59.203.26.25:1521:ORCL","sjkjdb","sjkj65334801"),
    //合和码
    //172.16.16.32 idata@2023
    HHM( "jdbc:mysql://localhost:3306/contradiction?useSSL=false", "root", "idata@2023"),
    //警民联调原始数据
    JMLT_ORIGIN( "jdbc:mysql://localhost:3306/jmlt_origin?useSSL=false", "root", "idata@2023"),
    //促法原始数据
    CF_ORIGIN( "jdbc:mysql://localhost:3306/cf_origin?useSSL=false", "root", "idata@2023");
    private String url;
    private String user;
    private String password;

    DataSource(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
