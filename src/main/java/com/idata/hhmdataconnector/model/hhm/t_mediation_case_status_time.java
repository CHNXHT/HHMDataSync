package com.idata.hhmdataconnector.model.hhm;

import java.io.Serializable;
import java.util.Date;

/**
 * 纠纷状态时间表

 * @TableName t_mediation_case_status_time
 */
public class t_mediation_case_status_time implements Serializable {
    /**
     * 主键id
     */
    private Long id;

    /**
     * 创建时间
     */
    private Date create_time;

    /**
     * 更新时间
     */
    private Date update_time;

    /**
     * 纠纷id
     */
    private Long case_id;

    /**
     * 纠纷类型
     */
    private String case_type;

    /**
     * 待受理状态时间
     */
    private Date to_accept_time;

    /**
     * 调解中状态时间
     */
    private Date dissolving_time;

    /**
     * 纠纷结束时间
     */
    private Date closed_time;

    /**
     * 调解成功时间
     */
    private Date success_time;

    /**
     * 调解结束时间
     */
    private Date ending_time;

    /**
     * 调解终止时间
     */
    private Date termination_time;

    private static final long serialVersionUID = 1L;

    /**
     * 主键id
     */
    public Long getId() {
        return id;
    }

    /**
     * 主键id
     */
    public void setId(Long id) {
        this.id = id;
    }

    /**
     * 创建时间
     */
    public Date getCreate_time() {
        return create_time;
    }

    /**
     * 创建时间
     */
    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    /**
     * 更新时间
     */
    public Date getUpdate_time() {
        return update_time;
    }

    /**
     * 更新时间
     */
    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }

    /**
     * 纠纷id
     */
    public Long getCase_id() {
        return case_id;
    }

    /**
     * 纠纷id
     */
    public void setCase_id(Long case_id) {
        this.case_id = case_id;
    }

    /**
     * 纠纷类型
     */
    public String getCase_type() {
        return case_type;
    }

    /**
     * 纠纷类型
     */
    public void setCase_type(String case_type) {
        this.case_type = case_type;
    }

    /**
     * 待受理状态时间
     */
    public Date getTo_accept_time() {
        return to_accept_time;
    }

    /**
     * 待受理状态时间
     */
    public void setTo_accept_time(Date to_accept_time) {
        this.to_accept_time = to_accept_time;
    }

    /**
     * 调解中状态时间
     */
    public Date getDissolving_time() {
        return dissolving_time;
    }

    /**
     * 调解中状态时间
     */
    public void setDissolving_time(Date dissolving_time) {
        this.dissolving_time = dissolving_time;
    }

    /**
     * 纠纷结束时间
     */
    public Date getClosed_time() {
        return closed_time;
    }

    /**
     * 纠纷结束时间
     */
    public void setClosed_time(Date closed_time) {
        this.closed_time = closed_time;
    }

    /**
     * 调解成功时间
     */
    public Date getSuccess_time() {
        return success_time;
    }

    /**
     * 调解成功时间
     */
    public void setSuccess_time(Date success_time) {
        this.success_time = success_time;
    }

    /**
     * 调解结束时间
     */
    public Date getEnding_time() {
        return ending_time;
    }

    /**
     * 调解结束时间
     */
    public void setEnding_time(Date ending_time) {
        this.ending_time = ending_time;
    }

    /**
     * 调解终止时间
     */
    public Date getTermination_time() {
        return termination_time;
    }

    /**
     * 调解终止时间
     */
    public void setTermination_time(Date termination_time) {
        this.termination_time = termination_time;
    }
}