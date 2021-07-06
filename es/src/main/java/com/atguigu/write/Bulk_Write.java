package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class Bulk_Write {
    public static void main(String[] args) throws IOException {
        //1.创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接参数
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取客户端连接
        JestClient jestClient = jestClientFactory.getObject();

        //4.批量写入数据
        Movie movie104 = new Movie("104", "金刚大战哥斯拉");
        Movie movie105 = new Movie("105", "超人大战蝙蝠侠");
        Movie movie106 = new Movie("106", "贞子大战伽椰子");


        Index index104 = new Index.Builder(movie104).id("1004").build();
        Index index105 = new Index.Builder(movie105).id("1005").build();
        Index index106 = new Index.Builder(movie106).id("1006").build();

        Bulk bulk = new Bulk.Builder()
                .addAction(index104)
                .addAction(index105)
                .addAction(index106)
                .defaultIndex("movie")
                .defaultType("_doc")
                .build();

        jestClient.execute(bulk);

        //关闭客户端连接
        jestClient.shutdownClient();
    }
}
