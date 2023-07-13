package com.idata.hhmdataconnector.model.hhm;

import java.io.Serializable;

/**
 * 案件日志表
 * @TableName t_mediation_case_log
 */
public class t_mediation_case_log implements Serializable {
    /**
     * 主键id
     */
    private Long id;

    /**
     * 创建时间
     */
    private String create_time;

    /**
     * 修改时间
     */
    private String update_time;

    /**
     * 纠纷案件id
     */
    private Long case_id;

    /**
     * 日志描述
     */
    private String log_description;

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
    public String getCreate_time() {
        return create_time;
    }

    /**
     * 创建时间
     */
    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    /**
     * 修改时间
     */
    public String getUpdate_time() {
        return update_time;
    }

    /**
     * 修改时间
     */
    public void setUpdate_time(String update_time) {
        this.update_time = update_time;
    }

    /**
     * 纠纷案件id
     */
    public Long getCase_id() {
        return case_id;
    }

    /**
     * 纠纷案件id
     */
    public void setCase_id(Long case_id) {
        this.case_id = case_id;
    }

    /**
     * 日志描述
     */
    public String getLog_description() {
        return log_description;
    }

    /**
     * 日志描述
     */
    public void setLog_description(String log_description) {
        this.log_description = log_description;
    }
}