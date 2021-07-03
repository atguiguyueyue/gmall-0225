package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //查询日活总数据
    public Integer selectDauTotal(String date);

    //查询分时日活数据
    public List<Map> selectDauTotalHourMap(String date);


}
