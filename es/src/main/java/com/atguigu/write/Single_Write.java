package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class Single_Write {
    public static void main(String[] args) throws IOException {
        //1.创建连接工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        Movie movie = new Movie("103","举起手来");

        //4.单条写入数据
        Index index = new Index.Builder(movie)
                .index("movie")
                .type("_doc")
                .id("1003")
                .build();

     /*   Index index = new Index.Builder("{\n" +
                "  \"id\":\"102\",\n" +
                "  \"movie_name\":\"欧美小电影\"\n" +
                "}\n")
                .index("movie")
                .type("_doc")
                .id("1002")
                .build();*/


        jestClient.execute(index);

        //TODO 关闭连接(close,有可能关不掉这个连接)
        jestClient.shutdownClient();
    }
}
