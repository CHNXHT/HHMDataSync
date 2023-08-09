package com.idata.hhmdataconnector.mapper;

import com.idata.hhmdataconnector.model.hhm.t_mediation_case;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
* @author chnxh
* @description 针对表【t_mediation_case(纠纷案件表)】的数据库操作Mapper
* @createDate 2023-08-08 16:59:36
* @Entity .model.t_mediation_case
*/
@Mapper
public interface t_mediation_caseMapper extends BaseMapper<t_mediation_case> {
    void updateOrInsertCaseInfo(List<t_mediation_case> tMediationCaseList);
}




