package com.idata.hhmdataconnector.plugin.xf;

import cn.hutool.core.date.DateUtil;
import com.idata.hhmdataconnector.mapper.t_mediation_caseMapper;
import com.idata.hhmdataconnector.model.hhm.t_mediation_case;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.tomcat.util.buf.StringUtils;
import org.bitlap.geocoding.Geocoding;
import org.bitlap.geocoding.model.Address;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static java.util.Arrays.asList;

@Component
public class XfCase {
//    public static void main(String[] args) {
//        getAll();
//    }
    @Autowired
    private t_mediation_caseMapper tMediationCaseMapper;

    public void getAll(){
        String excelFilePath = "C:\\Users\\chnxh\\Desktop\\合肥矛调\\信访\\信访件列表2023.xls";

        try (FileInputStream inputStream = new FileInputStream(new File(excelFilePath));
             Workbook workbook = WorkbookFactory.create(inputStream)) {

            int numberOfSheets = workbook.getNumberOfSheets();
            List<t_mediation_case> tMediationCasesList = new ArrayList<>();
            for (int i = 0; i < numberOfSheets; i++) {
                Sheet sheet = workbook.getSheetAt(i);
                int count = 0;
                for (Row row : sheet) {
                    t_mediation_case tMediationCase = new t_mediation_case();
                    String create_time = "",update_time = "",case_num="",case_description="",case_type="",
                            place_code="",resource_id="",place_detail="",create_user_name = "";
                    create_time = DateUtil.now();
                    update_time = create_time;
                    tMediationCase.setCreate_time(create_time);
                    tMediationCase.setUpdate_time(update_time);
                    tMediationCase.setCase_source(7);
                    tMediationCase.setMethod(2);
                    Long create_user_id = 10101L;
                    //case_num
                    if (row.getCell(3) != null) {
                        case_num = row.getCell(3).getStringCellValue();
                        tMediationCase.setCase_num(case_num);
                    }
                    //case_description 18
                    if (row.getCell(18) != null) {
                        case_description = row.getCell(18).getStringCellValue();
                        tMediationCase.setCase_description(case_description);
                    }
                    //case_type 12
                    if (row.getCell(12) != null) {
                        case_type = row.getCell(12).getStringCellValue();
                        tMediationCase.setCase_type(case_type);
                    }
                    //place_code,resource_id,place_detail 7
                    if (row.getCell(7) != null) {
                        String detail = row.getCell(7).getStringCellValue();
                        resource_id = detail;
                        place_detail = detail;
                        place_code = addressResolution(resource_id);
                        tMediationCase.setResource_id(resource_id);
                        tMediationCase.setPlace_detail(place_detail);
                        tMediationCase.setPlace_code(place_code);
                    }
                    //status 9
                    if (row.getCell(9) != null) {
                        int status = Integer.parseInt(row.getCell(9).getStringCellValue());
                        tMediationCase.setStatus(status);
                    }
                    //create_user_name 14
                    if (row.getCell(14) != null) {
                        create_user_name = row.getCell(14).getStringCellValue();
                        tMediationCase.setCreate_user_name(create_user_name);
                    }
                    tMediationCasesList.add(tMediationCase);
                    ++count;
                    //这里有问题，剩余数据怎么处理
                    if(count%200 == 0) {
                        tMediationCaseMapper.updateOrInsertCaseInfo(tMediationCasesList);
                        tMediationCasesList.clear();
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 解析地址
     * @author xiehaotian
     * @param detailAddress
     * @return
     */
    public static String addressResolution(String detailAddress){
        String place_code = "";
        //拼接5级
        Address address = Geocoding.normalizing(detailAddress);
        if (address != null){

            String provinceId = address.getProvinceId() != null ? address.getProvinceId().toString() : "";
            String cityId = address.getCityId() != null ? address.getCityId().toString() : "";
            String districtId = address.getDistrictId() != null ? address.getDistrictId().toString() : "";
            String townId = address.getTownId() != null ? address.getTownId().toString() : "";
            String villageId = address.getVillageId() != null ? address.getVillageId().toString() : "";
            List<String> list1 = asList(provinceId, cityId, districtId,townId,villageId);
            place_code = StringUtils.join(list1,',');

            String province = address.getProvince() != null ? address.getProvince() : "";
            String city = address.getCity() != null ? address.getCity() : "";
            String district = address.getDistrict() != null ? address.getDistrict() : "";
            String town = address.getTown() != null ? address.getTown() : "";
            String village = address.getVillage() != null ? address.getVillage() : "";
            List<String> list2 = asList(province, city, district,town,village);
            String place_code1 = StringUtils.join(list2,',');
//            if (!province.isEmpty() && province.equals("安徽省") && !city.isEmpty() && city.equals("合肥市"))
//                System.out.println(detailAddress + "======>" + place_code1 + "=====>" + place_code);

        }
        return place_code;
    }
}
