package com.atguigu.gmallpublisher.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.GmallConstants;
import com.atguigu.gmallpublisher.bean.Options;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;

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

    @Override
    public String getSaleDetail(String date, Integer start, Integer size, String keyWord) throws IOException {
        //获取数据
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤 匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyWord).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //  性别聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_user_gender").field("user_gender").size(2);
        searchSourceBuilder.aggregation(genderAggs);

        //  年龄聚合
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_user_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(ageAggs);

        // 行号= （页面-1） * 每页行数
        searchSourceBuilder.from((start - 1) * size);
        searchSourceBuilder.size(size);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstants.ES_SALE_DETAIL+"0225").addType("_doc").build();

        SearchResult searchResult = jestClient.execute(search);

        //TODO 1.获取命中条数（数据总数）
        Long total = searchResult.getTotal();

        //TODO 2.获取明细数据
        //创建list集合用来存放明细数据
        ArrayList<Map> detail = new ArrayList<>();
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            detail.add(source);
        }

        //TODO 3.聚合组数据
        MetricAggregation aggregations = searchResult.getAggregations();

        //TODO 年龄
        TermsAggregation groupby_user_age = aggregations.getTermsAggregation("groupby_user_age");
        //20岁以下的个数
        int low20Count = 0;
        //30岁以上的个数
        int up30Count = 0;
        List<TermsAggregation.Entry> buckets = groupby_user_age.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            if (Integer.parseInt(bucket.getKey())<20){
                low20Count += bucket.getCount();
            }else if (Integer.parseInt(bucket.getKey())>30){
                up30Count += bucket.getCount();
            }
        }
        //20岁以下的年龄占比
        Double low20Ratio = Math.round(low20Count * 1000D / total) / 10D;

        //30岁以上的年龄占比
        Double up30Ratio = Math.round(up30Count * 1000D / total) / 10D;

        //20-30的年龄占比
        Double up20Low30Ratio = Math.round((100D - low20Ratio - up30Ratio) * 10D) / 10D;

        //创建options对象用来存放数据
        Options low20Opt = new Options("20岁以下", low20Ratio);
        Options up20Low30Opt = new Options("20岁到30岁", up20Low30Ratio);
        Options up30Opt = new Options("30岁及30岁以上", up30Ratio);

        //创建存放年龄占比的List集合
        ArrayList<Options> ageList = new ArrayList<>();
        ageList.add(low20Opt);
        ageList.add(up20Low30Opt);
        ageList.add(up30Opt);

        //创建年龄占比相关的Stat对象
        Stat ageStat = new Stat(ageList, "用户年龄占比");

        //TODO 性别
        TermsAggregation groupby_user_gender = aggregations.getTermsAggregation("groupby_user_gender");
        List<TermsAggregation.Entry> genderBuckets = groupby_user_gender.getBuckets();
        //男性个数
        int maleCount = 0;
        for (TermsAggregation.Entry bucket : genderBuckets) {
            if ("M".equals(bucket.getKey())){
                maleCount += bucket.getCount();
            }
        }

        //男性占比
        double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        //女性占比
        double femaleRatio = Math.round((100D - maleRatio) * 10D) / 10D;

        //创建Options对象用来存放数据
        Options maleOpt = new Options("男", maleRatio);
        Options femaleOpt = new Options("女", femaleRatio);

        //创建存放性别的List集合
        ArrayList<Options> genderList = new ArrayList<>();
        genderList.add(maleOpt);
        genderList.add(femaleOpt);

        //创建性别相关的Stat对象
        Stat genderStat = new Stat(genderList, "用户性别占比");

        //创建存放Stat对象的集合
        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);

        //创建Map集合用来存放最终的结果数据
        HashMap<String, Object> result = new HashMap<>();

        result.put("total", total);
        result.put("stat", stats);
        result.put("detail", detail);

        return JSONObject.toJSONString(result);
    }
}
