package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {

    //处理日活总数据抽象方法
    public Integer getDauTotal(String date);

    //处理分时数据的抽象方法
    public Map getDauTotalHour(String date);
}
