package com.idata.hhmdataconnector.model.hhm;

import java.io.Serializable;

/**
 * 申请人和被申请人信息
 * @TableName t_mediation_case_people
 */
public class t_mediation_case_people implements Serializable {
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
     * 参与人类型：1 申请人、2 被申请人
     */
    private Integer clazz;

    /**
     * 申请人、被申请人类型：1 自然人、2 法人、3 非法人组织
     */
    private Integer type;

    /**
     * 申请人姓名/企业名称
     */
    private String name;

    /**
     * 自然人证件类型：1 居民身份证、2 护照
     */
    private Integer identity_type;

    /**
     * 自然人证件号码
     */
    private String identity_num;

    /**
     * 自然人性别：1 男性、2 女性
     */
    private Integer gender;

    /**
     * 联系电话
     */
    private String phone;

    /**
     * 地址-国家行政区代码
     */
    private String place_code;

    /**
     * 地址-详细地址
     */
    private String place_detail;

    /**
     * 社会信用码
     */
    private String credit_code;

    /**
     * 法定代表人
     */
    private String legal_representative;

    /**
     * 申请人/被申请人年龄
     */
    private String age;

    /**
     * 民族
     */
    private String nation;

    /**
     * 职位/职务
     */
    private String position;

    /**
     * 纠纷案件id
     */
    private Long case_id;

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
     * 参与人类型：1 申请人、2 被申请人
     */
    public Integer getClazz() {
        return clazz;
    }

    /**
     * 参与人类型：1 申请人、2 被申请人
     */
    public void setClass(Integer clazz) {
        this.clazz = clazz;
    }

    /**
     * 申请人、被申请人类型：1 自然人、2 法人、3 非法人组织
     */
    public Integer getType() {
        return type;
    }

    /**
     * 申请人、被申请人类型：1 自然人、2 法人、3 非法人组织
     */
    public void setType(Integer type) {
        this.type = type;
    }

    /**
     * 申请人姓名/企业名称
     */
    public String getName() {
        return name;
    }

    /**
     * 申请人姓名/企业名称
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 自然人证件类型：1 居民身份证、2 护照
     */
    public Integer getIdentity_type() {
        return identity_type;
    }

    /**
     * 自然人证件类型：1 居民身份证、2 护照
     */
    public void setIdentity_type(Integer identity_type) {
        this.identity_type = identity_type;
    }

    /**
     * 自然人证件号码
     */
    public String getIdentity_num() {
        return identity_num;
    }

    /**
     * 自然人证件号码
     */
    public void setIdentity_num(String identity_num) {
        this.identity_num = identity_num;
    }

    /**
     * 自然人性别：1 男性、2 女性
     */
    public Integer getGender() {
        return gender;
    }

    /**
     * 自然人性别：1 男性、2 女性
     */
    public void setGender(Integer gender) {
        this.gender = gender;
    }

    /**
     * 联系电话
     */
    public String getPhone() {
        return phone;
    }

    /**
     * 联系电话
     */
    public void setPhone(String phone) {
        this.phone = phone;
    }

    /**
     * 地址-国家行政区代码
     */
    public String getPlace_code() {
        return place_code;
    }

    /**
     * 地址-国家行政区代码
     */
    public void setPlace_code(String place_code) {
        this.place_code = place_code;
    }

    /**
     * 地址-详细地址
     */
    public String getPlace_detail() {
        return place_detail;
    }

    /**
     * 地址-详细地址
     */
    public void setPlace_detail(String place_detail) {
        this.place_detail = place_detail;
    }

    /**
     * 社会信用码
     */
    public String getCredit_code() {
        return credit_code;
    }

    /**
     * 社会信用码
     */
    public void setCredit_code(String credit_code) {
        this.credit_code = credit_code;
    }

    /**
     * 法定代表人
     */
    public String getLegal_representative() {
        return legal_representative;
    }

    /**
     * 法定代表人
     */
    public void setLegal_representative(String legal_representative) {
        this.legal_representative = legal_representative;
    }

    /**
     * 申请人/被申请人年龄
     */
    public String getAge() {
        return age;
    }

    /**
     * 申请人/被申请人年龄
     */
    public void setAge(String age) {
        this.age = age;
    }

    /**
     * 民族
     */
    public String getNation() {
        return nation;
    }

    /**
     * 民族
     */
    public void setNation(String nation) {
        this.nation = nation;
    }

    /**
     * 职位/职务
     */
    public String getPosition() {
        return position;
    }

    /**
     * 职位/职务
     */
    public void setPosition(String position) {
        this.position = position;
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
}