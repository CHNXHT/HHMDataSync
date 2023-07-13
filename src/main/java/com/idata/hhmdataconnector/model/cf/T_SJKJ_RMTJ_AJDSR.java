package com.idata.hhmdataconnector.model.cf;

import java.io.Serializable;

/**
 * 
 * @TableName T_SJKJ_RMTJ_AJDSR
 */
public class T_SJKJ_RMTJ_AJDSR implements Serializable {
    /**
     * 案件编号
     */
    private String AJBH;

    /**
     * 当事人地位
     */
    private String DSRDW;

    /**
     * 当事人类型
     */
    private String DSRLX;

    /**
     * 当事人证件类型
     */
    private String DSRZJLX;

    /**
     * 当事人证件号码
     */
    private String DSRZJHM;

    /**
     * 姓名名称
     */
    private String XMMC;

    /**
     * 当事人联系电话
     */
    private String DSRLXDH;

    /**
     * 名族
     */
    private String MZ;

    /**
     * 年龄
     */
    private String NL;

    /**
     * 当事人地址
     */
    private String DSRDZ;

    private static final long serialVersionUID = 1L;

    /**
     * 案件编号
     */
    public String getAJBH() {
        return AJBH;
    }

    /**
     * 案件编号
     */
    public void setAJBH(String AJBH) {
        this.AJBH = AJBH;
    }

    /**
     * 当事人地位
     */
    public String getDSRDW() {
        return DSRDW;
    }

    /**
     * 当事人地位
     */
    public void setDSRDW(String DSRDW) {
        this.DSRDW = DSRDW;
    }

    /**
     * 当事人类型
     */
    public String getDSRLX() {
        return DSRLX;
    }

    /**
     * 当事人类型
     */
    public void setDSRLX(String DSRLX) {
        this.DSRLX = DSRLX;
    }

    /**
     * 当事人证件类型
     */
    public String getDSRZJLX() {
        return DSRZJLX;
    }

    /**
     * 当事人证件类型
     */
    public void setDSRZJLX(String DSRZJLX) {
        this.DSRZJLX = DSRZJLX;
    }

    /**
     * 当事人证件号码
     */
    public String getDSRZJHM() {
        return DSRZJHM;
    }

    /**
     * 当事人证件号码
     */
    public void setDSRZJHM(String DSRZJHM) {
        this.DSRZJHM = DSRZJHM;
    }

    /**
     * 姓名名称
     */
    public String getXMMC() {
        return XMMC;
    }

    /**
     * 姓名名称
     */
    public void setXMMC(String XMMC) {
        this.XMMC = XMMC;
    }

    /**
     * 当事人联系电话
     */
    public String getDSRLXDH() {
        return DSRLXDH;
    }

    /**
     * 当事人联系电话
     */
    public void setDSRLXDH(String DSRLXDH) {
        this.DSRLXDH = DSRLXDH;
    }

    /**
     * 名族
     */
    public String getMZ() {
        return MZ;
    }

    /**
     * 名族
     */
    public void setMZ(String MZ) {
        this.MZ = MZ;
    }

    /**
     * 年龄
     */
    public String getNL() {
        return NL;
    }

    /**
     * 年龄
     */
    public void setNL(String NL) {
        this.NL = NL;
    }

    /**
     * 当事人地址
     */
    public String getDSRDZ() {
        return DSRDZ;
    }

    /**
     * 当事人地址
     */
    public void setDSRDZ(String DSRDZ) {
        this.DSRDZ = DSRDZ;
    }
}