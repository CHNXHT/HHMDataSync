package com.idata.hhmdataconnector.model.hhm;

/**
 * 纠纷案件表
 * @TableName t_mediation_case
 */
public class t_mediation_case{
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
     * 案件号
     */
    private String case_num;

    /**
     * 纠纷描述
     */
    private String case_description;

    /**
     * 纠纷类型：数据字典code
     */
    private String case_type;

    /**
     * 纠纷发生地-国家行政区代码
     */
    private String place_code;

    /**
     * 纠纷发生地-详细地址
     */
    private String place_detail;

    /**
     * 纠纷发生日期
     */
    private String occurrence_time;

    /**
     * 请求事项
     */
    private String request;

    /**
     * 调解方式：1 自行调解 、2 第三方调解
     */
    private Integer method;

    /**
     * 证据材料说明
     */
    private String evidence_description;

    /**
     * 创建人id
     */
    private Long create_user_id;

    /**
     * 案件状态：1 待受理、2 化解中、3 案件结束
     */
    private Integer status;

    /**
     * 调解结果：1 调解成功、2 调解失败、3 调解终止
     */
    private Integer result;

    /**
     * 案件文书状态：1 生成文书、2 生成全部签字文书、3 废弃全部文书
     */
    private Integer doc_status;

    /**
     * 纠纷来源
     */
    private String case_resource;

    /**
     * 纠纷来源表的主键id
     */
    private String resource_id;

    /**
     * 创建人姓名
     */
    private String create_user_name;

    /**
     * 纠纷来源，0-和合码、1-警民联调、2-促法平台、3-人民调解平台、4-审判管理系统、5-劳动仲裁系统、6-仲裁委案件系统、7省信访平台、8-12345热线平台、9-其他
     */
    private Integer case_source;

    /**
     * 纠纷级别，0-一般,1-疑难、2-重大
     */
    private Integer case_level;

    /**
     * 涉及人数
     */
    private String involved_people;

    /**
     * 重复来访，0-初次,1-重复来访
     */
    private Integer repeat_visit;

    /**
     * 其他情况
     */
    private Integer other_situation;

    /**
     * 特殊说明
     */
    private String special_instructions;

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
     * 案件号
     */
    public String getCase_num() {
        return case_num;
    }

    /**
     * 案件号
     */
    public void setCase_num(String case_num) {
        this.case_num = case_num;
    }

    /**
     * 纠纷描述
     */
    public String getCase_description() {
        return case_description;
    }

    /**
     * 纠纷描述
     */
    public void setCase_description(String case_description) {
        this.case_description = case_description;
    }

    /**
     * 纠纷类型：数据字典code
     */
    public String getCase_type() {
        return case_type;
    }

    /**
     * 纠纷类型：数据字典code
     */
    public void setCase_type(String case_type) {
        this.case_type = case_type;
    }

    /**
     * 纠纷发生地-国家行政区代码
     */
    public String getPlace_code() {
        return place_code;
    }

    /**
     * 纠纷发生地-国家行政区代码
     */
    public void setPlace_code(String place_code) {
        this.place_code = place_code;
    }

    /**
     * 纠纷发生地-详细地址
     */
    public String getPlace_detail() {
        return place_detail;
    }

    /**
     * 纠纷发生地-详细地址
     */
    public void setPlace_detail(String place_detail) {
        this.place_detail = place_detail;
    }

    /**
     * 纠纷发生日期
     */
    public String getOccurrence_time() {
        return occurrence_time;
    }

    /**
     * 纠纷发生日期
     */
    public void setOccurrence_time(String occurrence_time) {
        this.occurrence_time = occurrence_time;
    }

    /**
     * 请求事项
     */
    public String getRequest() {
        return request;
    }

    /**
     * 请求事项
     */
    public void setRequest(String request) {
        this.request = request;
    }

    /**
     * 调解方式：1 自行调解 、2 第三方调解
     */
    public Integer getMethod() {
        return method;
    }

    /**
     * 调解方式：1 自行调解 、2 第三方调解
     */
    public void setMethod(Integer method) {
        this.method = method;
    }

    /**
     * 证据材料说明
     */
    public String getEvidence_description() {
        return evidence_description;
    }

    /**
     * 证据材料说明
     */
    public void setEvidence_description(String evidence_description) {
        this.evidence_description = evidence_description;
    }

    /**
     * 创建人id
     */
    public Long getCreate_user_id() {
        return create_user_id;
    }

    /**
     * 创建人id
     */
    public void setCreate_user_id(Long create_user_id) {
        this.create_user_id = create_user_id;
    }

    /**
     * 案件状态：1 待受理、2 化解中、3 案件结束
     */
    public Integer getStatus() {
        return status;
    }

    /**
     * 案件状态：1 待受理、2 化解中、3 案件结束
     */
    public void setStatus(Integer status) {
        this.status = status;
    }

    /**
     * 调解结果：1 调解成功、2 调解失败、3 调解终止
     */
    public Integer getResult() {
        return result;
    }

    /**
     * 调解结果：1 调解成功、2 调解失败、3 调解终止
     */
    public void setResult(Integer result) {
        this.result = result;
    }

    /**
     * 案件文书状态：1 生成文书、2 生成全部签字文书、3 废弃全部文书
     */
    public Integer getDoc_status() {
        return doc_status;
    }

    /**
     * 案件文书状态：1 生成文书、2 生成全部签字文书、3 废弃全部文书
     */
    public void setDoc_status(Integer doc_status) {
        this.doc_status = doc_status;
    }

    /**
     * 纠纷来源
     */
    public String getCase_resource() {
        return case_resource;
    }

    /**
     * 纠纷来源
     */
    public void setCase_resource(String case_resource) {
        this.case_resource = case_resource;
    }

    /**
     * 纠纷来源表的主键id
     */
    public String getResource_id() {
        return resource_id;
    }

    /**
     * 纠纷来源表的主键id
     */
    public void setResource_id(String resource_id) {
        this.resource_id = resource_id;
    }

    /**
     * 创建人姓名
     */
    public String getCreate_user_name() {
        return create_user_name;
    }

    /**
     * 创建人姓名
     */
    public void setCreate_user_name(String create_user_name) {
        this.create_user_name = create_user_name;
    }

    /**
     * 纠纷来源，0-和合码、1-警民联调、2-促法平台、3-人民调解平台、4-审判管理系统、5-劳动仲裁系统、6-仲裁委案件系统、7省信访平台、8-12345热线平台、9-其他
     */
    public Integer getCase_source() {
        return case_source;
    }

    /**
     * 纠纷来源，0-和合码、1-警民联调、2-促法平台、3-人民调解平台、4-审判管理系统、5-劳动仲裁系统、6-仲裁委案件系统、7省信访平台、8-12345热线平台、9-其他
     */
    public void setCase_source(Integer case_source) {
        this.case_source = case_source;
    }

    /**
     * 纠纷级别，0-一般,1-疑难、2-重大
     */
    public Integer getCase_level() {
        return case_level;
    }

    /**
     * 纠纷级别，0-一般,1-疑难、2-重大
     */
    public void setCase_level(Integer case_level) {
        this.case_level = case_level;
    }

    /**
     * 涉及人数
     */
    public String getInvolved_people() {
        return involved_people;
    }

    /**
     * 涉及人数
     */
    public void setInvolved_people(String involved_people) {
        this.involved_people = involved_people;
    }

    /**
     * 重复来访，0-初次,1-重复来访
     */
    public Integer getRepeat_visit() {
        return repeat_visit;
    }

    /**
     * 重复来访，0-初次,1-重复来访
     */
    public void setRepeat_visit(Integer repeat_visit) {
        this.repeat_visit = repeat_visit;
    }

    /**
     * 其他情况
     */
    public Integer getOther_situation() {
        return other_situation;
    }

    /**
     * 其他情况
     */
    public void setOther_situation(Integer other_situation) {
        this.other_situation = other_situation;
    }

    /**
     * 特殊说明
     */
    public String getSpecial_instructions() {
        return special_instructions;
    }

    /**
     * 特殊说明
     */
    public void setSpecial_instructions(String special_instructions) {
        this.special_instructions = special_instructions;
    }
}