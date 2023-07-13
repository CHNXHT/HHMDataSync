package com.idata.hhmdataconnector.model.hhm;

import java.io.Serializable;
import java.util.Date;

/**
 * 机构表
 * @TableName t_organization
 */
public class t_organization implements Serializable {
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
     * 机构名称
     */
    private String name;

    /**
     * 类型
     */
    private Integer type;

    /**
     * 省、直辖市编码
     */
    private String province;

    /**
     * 市编码
     */
    private String city;

    /**
     * 区、县编码
     */
    private String county;

    /**
     * 乡、镇、街道编码
     */
    private String town;

    /**
     * 村、社区、居委会编码
     */
    private String village;

    /**
     * 详细地址
     */
    private String address;

    /**
     * 负责人
     */
    private String charge_person;

    /**
     * 电话
     */
    private String phone;

    /**
     * 上级机构id
     */
    private Long parent_id;

    /**
     * 是否删除 1:删除 0:正常
     */
    private Integer del_flag;

    /**
     * 文件图片存放路径
     */
    private String seal_path;

    /**
     * 讯飞机构id
     */
    private String xf_org_id;

    /**
     * 讯飞父级id
     */
    private String xf_parent_id;

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
     * 机构名称
     */
    public String getName() {
        return name;
    }

    /**
     * 机构名称
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 类型
     */
    public Integer getType() {
        return type;
    }

    /**
     * 类型
     */
    public void setType(Integer type) {
        this.type = type;
    }

    /**
     * 省、直辖市编码
     */
    public String getProvince() {
        return province;
    }

    /**
     * 省、直辖市编码
     */
    public void setProvince(String province) {
        this.province = province;
    }

    /**
     * 市编码
     */
    public String getCity() {
        return city;
    }

    /**
     * 市编码
     */
    public void setCity(String city) {
        this.city = city;
    }

    /**
     * 区、县编码
     */
    public String getCounty() {
        return county;
    }

    /**
     * 区、县编码
     */
    public void setCounty(String county) {
        this.county = county;
    }

    /**
     * 乡、镇、街道编码
     */
    public String getTown() {
        return town;
    }

    /**
     * 乡、镇、街道编码
     */
    public void setTown(String town) {
        this.town = town;
    }

    /**
     * 村、社区、居委会编码
     */
    public String getVillage() {
        return village;
    }

    /**
     * 村、社区、居委会编码
     */
    public void setVillage(String village) {
        this.village = village;
    }

    /**
     * 详细地址
     */
    public String getAddress() {
        return address;
    }

    /**
     * 详细地址
     */
    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * 负责人
     */
    public String getCharge_person() {
        return charge_person;
    }

    /**
     * 负责人
     */
    public void setCharge_person(String charge_person) {
        this.charge_person = charge_person;
    }

    /**
     * 电话
     */
    public String getPhone() {
        return phone;
    }

    /**
     * 电话
     */
    public void setPhone(String phone) {
        this.phone = phone;
    }

    /**
     * 上级机构id
     */
    public Long getParent_id() {
        return parent_id;
    }

    /**
     * 上级机构id
     */
    public void setParent_id(Long parent_id) {
        this.parent_id = parent_id;
    }

    /**
     * 是否删除 1:删除 0:正常
     */
    public Integer getDel_flag() {
        return del_flag;
    }

    /**
     * 是否删除 1:删除 0:正常
     */
    public void setDel_flag(Integer del_flag) {
        this.del_flag = del_flag;
    }

    /**
     * 文件图片存放路径
     */
    public String getSeal_path() {
        return seal_path;
    }

    /**
     * 文件图片存放路径
     */
    public void setSeal_path(String seal_path) {
        this.seal_path = seal_path;
    }

    /**
     * 讯飞机构id
     */
    public String getXf_org_id() {
        return xf_org_id;
    }

    /**
     * 讯飞机构id
     */
    public void setXf_org_id(String xf_org_id) {
        this.xf_org_id = xf_org_id;
    }

    /**
     * 讯飞父级id
     */
    public String getXf_parent_id() {
        return xf_parent_id;
    }

    /**
     * 讯飞父级id
     */
    public void setXf_parent_id(String xf_parent_id) {
        this.xf_parent_id = xf_parent_id;
    }
}