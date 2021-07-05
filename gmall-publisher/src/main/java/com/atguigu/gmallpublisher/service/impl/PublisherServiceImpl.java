package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHour(String date) {
        //1.获取数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //1.2创建map集合用来存放新的数据
        HashMap<String, Long> hourMap = new HashMap<>();

        //2.遍历list集合
        for (Map map : list) {
            hourMap.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return hourMap;
    }

    @Override
    public Double getGmvTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getGmvTotalHour(String date) {
        //1.获取数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //1.2创建新的Map集合用来存放新的结构(Hour->value)数据
        HashMap<String, Double> result = new HashMap<>();

        //2.遍历list集合，获取到里面原始的每个Map
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        return result;
    }
}
