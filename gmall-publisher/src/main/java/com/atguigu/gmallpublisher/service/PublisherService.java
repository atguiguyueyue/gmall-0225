package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {

    //处理日活总数据抽象方法
    public Integer getDauTotal(String date);

    //处理分时数据的抽象方法
    public Map getDauTotalHour(String date);

    //处理交易额总数数据抽象方法
    public Double getGmvTotal(String date);

    //处理交易额分时数据抽象方法
    public Map getGmvTotalHour(String date);
}
