package com.idata.hhmdataconnector.model.hhm;

import java.io.Serializable;

/**
 * 案件流转参与者，如：调解机构管理员（唯一）、主调解员（唯一）、协同调解员（多个）
 * @TableName t_mediation_participant
 */
public class t_mediation_participant implements Serializable {
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
     * 案件流转参与者id
     */
    private Long user_id;

    /**
     * 案件流转参与者所在调解机构id
     */
    private Long org_id;

    /**
     * 纠纷案件id
     */
    private Long case_id;

    /**
     * 调解机构/调解员标识：1 调解机构、 2 调解员、3 协同调解员
     */
    private Integer flag;

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
     * 案件流转参与者id
     */
    public Long getUser_id() {
        return user_id;
    }

    /**
     * 案件流转参与者id
     */
    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    /**
     * 案件流转参与者所在调解机构id
     */
    public Long getOrg_id() {
        return org_id;
    }

    /**
     * 案件流转参与者所在调解机构id
     */
    public void setOrg_id(Long org_id) {
        this.org_id = org_id;
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
     * 调解机构/调解员标识：1 调解机构、 2 调解员、3 协同调解员
     */
    public Integer getFlag() {
        return flag;
    }

    /**
     * 调解机构/调解员标识：1 调解机构、 2 调解员、3 协同调解员
     */
    public void setFlag(Integer flag) {
        this.flag = flag;
    }
}