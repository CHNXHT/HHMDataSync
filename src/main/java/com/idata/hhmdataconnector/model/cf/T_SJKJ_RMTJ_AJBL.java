package com.idata.hhmdataconnector.model.cf;

import java.io.Serializable;

/**
 * 
 * @TableName T_SJKJ_RMTJ_AJBL
 */
public class T_SJKJ_RMTJ_AJBL implements Serializable {
    /**
     * 纠纷类型
     */
    private String JFLX;

    /**
     * 纠纷来源
     */
    private String JFLY;

    /**
     * 行政区划
     */
    private String XZQH;

    /**
     * 受理日期
     */
    private String SLRQ;

    /**
     * 发生日期
     */
    private String FSRQ;

    /**
     * 
     */
    private String SADSRSL;

    /**
     * 涉案金额
     */
    private Long SAJE;

    /**
     * 涉及特殊群体情况
     */
    private String SJTSQTQK;

    /**
     * 涉及农民工纠纷情况
     */
    private String SJNMGJFQK;

    /**
     * 编号
     */
    private String BH;

    /**
     * 受理单位
     */
    private String SLDW;

    /**
     * 受理人
     */
    private String SLR;

    /**
     * 调解员
     */
    private String TJY;

    /**
     * 调解结果
     */
    private String TJJG;

    /**
     * 案件难度级别 
     */
    private String AJNDJB;

    /**
     * 纠纷转化情况 
     */
    private String JFJHQK;

    /**
     * 
     */
    private String AJYC;

    /**
     * 协议类型
     */
    private String XYLX;

    /**
     * 履行情况
     */
    private String LXQK;

    /**
     * 履行方式
     */
    private String LXFS;

    /**
     * 是否确认
     */
    private String SFSFQR;

    /**
     * 是否满意
     */
    private String SFMY;

    /**
     * 回访形式
     */
    private String HFXS;

    /**
     * 回访日期
     */
    private String HFRQ;

    /**
     * 案件状态
     */
    private String AJZT;

    /**
     * 案件编号
     */
    private String AJBH;

    /**
     * 纠纷简介
     */
    private String JFJJ;

    /**
     * 协议内容
     */
    private String XYNR;

    private static final long serialVersionUID = 1L;

    /**
     * 纠纷类型
     */
    public String getJFLX() {
        return JFLX;
    }

    /**
     * 纠纷类型
     */
    public void setJFLX(String JFLX) {
        this.JFLX = JFLX;
    }

    /**
     * 纠纷来源
     */
    public String getJFLY() {
        return JFLY;
    }

    /**
     * 纠纷来源
     */
    public void setJFLY(String JFLY) {
        this.JFLY = JFLY;
    }

    /**
     * 行政区划
     */
    public String getXZQH() {
        return XZQH;
    }

    /**
     * 行政区划
     */
    public void setXZQH(String XZQH) {
        this.XZQH = XZQH;
    }

    /**
     * 受理日期
     */
    public String getSLRQ() {
        return SLRQ;
    }

    /**
     * 受理日期
     */
    public void setSLRQ(String SLRQ) {
        this.SLRQ = SLRQ;
    }

    /**
     * 发生日期
     */
    public String getFSRQ() {
        return FSRQ;
    }

    /**
     * 发生日期
     */
    public void setFSRQ(String FSRQ) {
        this.FSRQ = FSRQ;
    }

    /**
     * 
     */
    public String getSADSRSL() {
        return SADSRSL;
    }

    /**
     * 
     */
    public void setSADSRSL(String SADSRSL) {
        this.SADSRSL = SADSRSL;
    }

    /**
     * 涉案金额
     */
    public Long getSAJE() {
        return SAJE;
    }

    /**
     * 涉案金额
     */
    public void setSAJE(Long SAJE) {
        this.SAJE = SAJE;
    }

    /**
     * 涉及特殊群体情况
     */
    public String getSJTSQTQK() {
        return SJTSQTQK;
    }

    /**
     * 涉及特殊群体情况
     */
    public void setSJTSQTQK(String SJTSQTQK) {
        this.SJTSQTQK = SJTSQTQK;
    }

    /**
     * 涉及农民工纠纷情况
     */
    public String getSJNMGJFQK() {
        return SJNMGJFQK;
    }

    /**
     * 涉及农民工纠纷情况
     */
    public void setSJNMGJFQK(String SJNMGJFQK) {
        this.SJNMGJFQK = SJNMGJFQK;
    }

    /**
     * 编号
     */
    public String getBH() {
        return BH;
    }

    /**
     * 编号
     */
    public void setBH(String BH) {
        this.BH = BH;
    }

    /**
     * 受理单位
     */
    public String getSLDW() {
        return SLDW;
    }

    /**
     * 受理单位
     */
    public void setSLDW(String SLDW) {
        this.SLDW = SLDW;
    }

    /**
     * 受理人
     */
    public String getSLR() {
        return SLR;
    }

    /**
     * 受理人
     */
    public void setSLR(String SLR) {
        this.SLR = SLR;
    }

    /**
     * 调解员
     */
    public String getTJY() {
        return TJY;
    }

    /**
     * 调解员
     */
    public void setTJY(String TJY) {
        this.TJY = TJY;
    }

    /**
     * 调解结果
     */
    public String getTJJG() {
        return TJJG;
    }

    /**
     * 调解结果
     */
    public void setTJJG(String TJJG) {
        this.TJJG = TJJG;
    }

    /**
     * 案件难度级别 
     */
    public String getAJNDJB() {
        return AJNDJB;
    }

    /**
     * 案件难度级别 
     */
    public void setAJNDJB(String AJNDJB) {
        this.AJNDJB = AJNDJB;
    }

    /**
     * 纠纷转化情况 
     */
    public String getJFJHQK() {
        return JFJHQK;
    }

    /**
     * 纠纷转化情况 
     */
    public void setJFJHQK(String JFJHQK) {
        this.JFJHQK = JFJHQK;
    }

    /**
     * 
     */
    public String getAJYC() {
        return AJYC;
    }

    /**
     * 
     */
    public void setAJYC(String AJYC) {
        this.AJYC = AJYC;
    }

    /**
     * 协议类型
     */
    public String getXYLX() {
        return XYLX;
    }

    /**
     * 协议类型
     */
    public void setXYLX(String XYLX) {
        this.XYLX = XYLX;
    }

    /**
     * 履行情况
     */
    public String getLXQK() {
        return LXQK;
    }

    /**
     * 履行情况
     */
    public void setLXQK(String LXQK) {
        this.LXQK = LXQK;
    }

    /**
     * 履行方式
     */
    public String getLXFS() {
        return LXFS;
    }

    /**
     * 履行方式
     */
    public void setLXFS(String LXFS) {
        this.LXFS = LXFS;
    }

    /**
     * 是否确认
     */
    public String getSFSFQR() {
        return SFSFQR;
    }

    /**
     * 是否确认
     */
    public void setSFSFQR(String SFSFQR) {
        this.SFSFQR = SFSFQR;
    }

    /**
     * 是否满意
     */
    public String getSFMY() {
        return SFMY;
    }

    /**
     * 是否满意
     */
    public void setSFMY(String SFMY) {
        this.SFMY = SFMY;
    }

    /**
     * 回访形式
     */
    public String getHFXS() {
        return HFXS;
    }

    /**
     * 回访形式
     */
    public void setHFXS(String HFXS) {
        this.HFXS = HFXS;
    }

    /**
     * 回访日期
     */
    public String getHFRQ() {
        return HFRQ;
    }

    /**
     * 回访日期
     */
    public void setHFRQ(String HFRQ) {
        this.HFRQ = HFRQ;
    }

    /**
     * 案件状态
     */
    public String getAJZT() {
        return AJZT;
    }

    /**
     * 案件状态
     */
    public void setAJZT(String AJZT) {
        this.AJZT = AJZT;
    }

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
     * 纠纷简介
     */
    public String getJFJJ() {
        return JFJJ;
    }

    /**
     * 纠纷简介
     */
    public void setJFJJ(String JFJJ) {
        this.JFJJ = JFJJ;
    }

    /**
     * 协议内容
     */
    public String getXYNR() {
        return XYNR;
    }

    /**
     * 协议内容
     */
    public void setXYNR(String XYNR) {
        this.XYNR = XYNR;
    }
}