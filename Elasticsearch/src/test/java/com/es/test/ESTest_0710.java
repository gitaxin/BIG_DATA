package com.es.test;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by Axin in 2019/10/11 21:13
 */
public class ESTest_0710 {


    private TransportClient client;

    @Before
    public void getClient() throws UnknownHostException {

    //1.设置连接的集群名称
        Settings settings = Settings.builder()
                .put("cluster.name","my-application")
                .build();

    //2.连接集群
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(
                InetAddress.getByName("192.168.110.111"),9300
        ));

        //3.打印集群名称
        System.out.println(client.toString());

    }

    @Test
    public void createIndex_blog() {
        //创建索引（库）
        client.admin().indices().prepareCreate("blog").get();
        client.close();

    }
    @Test
    public void deleteIndex() {
        //创建索引
        client.admin().indices().prepareDelete("blog").get();
        client.close();
    }


    @Test
    public void insertByJson() {
        //使用json创建document
        //1.文档数据准备
        //json中的id是业务中的id
        String json = "{" + "\"id\":\"1\"," + "\"title\":\"基于Lucene的搜索服务器\","
                + "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"" + "}";

        //2.在es中创建文档
        //index:库
        //type:表
        //id:es中的id。和上面的json中的id不是必须相同的。
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "1")
                .setSource(json).execute().actionGet();


        //3.打印返回结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("result:" + indexResponse.getResult());

        client.close();
    }

    @Test
    public void insertByMap() {
        //使用map创建document
        //1.文档 数据准备
        Map<String, Object> map = new HashMap<>();
        map.put("id", 2);
        map.put("title", "基于Lucene的搜索服务器");
        map.put("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");

        //2.创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "2").setSource(map).execute().actionGet();

        //3.打印返回结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("result:" + indexResponse.getResult());

        client.close();

    }
    @Test
    public void insertByXContent() throws IOException {
        //使用XContentBuilder创建document
        //1.通过es自带的帮助类，构建json数据
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id",1)
                .field("title","基于Lucene的搜索服务器")
                .field("content","我国火星探测器“真容”首度公开！关于取什么名字网友吵翻了")
                .endObject();

        //2.创建文档
        IndexResponse indexResponse = client.prepareIndex("blog2", "article", "1").setSource(builder).execute().actionGet();

        //3.打印返回结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("result:" + indexResponse.getResult());

        client.close();
    }



    @Test
    public void getData() {
        //1.查询文档
        GetResponse response = client.prepareGet("blog", "article", "1").get();
        System.out.println(response.getSourceAsString());
        client.close();


    }

    @Test
    public void getMultiData() {
        //查询多个文档
        MultiGetResponse responses = client.prepareMultiGet()
                .add("blog", "article", "1")
                .add("blog", "article", "2", "3")
                .add("blog", "article", "2")
                .get();
        for (MultiGetItemResponse res : responses) {
            GetResponse response = res.getResponse();
            if (response.isExists()) {
                System.out.println(response.getSourceAsString());

            }
        }
        client.close();

    }

    @Test
    public void updateData() throws IOException, ExecutionException, InterruptedException {
        //创建要更新的数据对象
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id",3)
                .field("title","基于Lucene的搜索服务器")
                .field("createDate","2019-10-11")
                .endObject();


        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("blog");
        updateRequest.type("article");
        updateRequest.id("3");

        updateRequest.doc(builder);


       //获取更新以后的值
        UpdateResponse updateResponse = client.update(updateRequest).get();

        //打印结果
        System.out.println("index:" + updateResponse.getIndex());
        System.out.println("type:" + updateResponse.getType());
        System.out.println("id:" + updateResponse.getId());
        System.out.println("result:" + updateResponse.getResult());

        client.close();

    }

    @Test
    public void testUpdateOrInsert() throws IOException, ExecutionException, InterruptedException {

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id",3)
                .field("content","它提供了一个分布式多用户能力的全文搜索引擎")
                .endObject();

        //设置查询条件，查找不到则添加
        //先查找5存不存在，如果存在则更新，不存在则插入
        IndexRequest indexRequest = new IndexRequest("blog","article","5")
                .source(builder);

        XContentBuilder builder2 = XContentFactory.jsonBuilder()
                .startObject()
                .field("user","李四")
                .endObject();
        //设置更新
        UpdateRequest upsert = new UpdateRequest("blog","article","5")
                .doc(builder2);
        client.update(upsert).get();
        client.close();

    }

    @Test
    public void deleteData() {
        //准备删除数据并删除
        DeleteResponse deleteResponse = client.prepareDelete("blog", "article", "5").get();
        //打印结果
        System.out.println("index:" + deleteResponse.getIndex());
        System.out.println("type:" + deleteResponse.getType());
        System.out.println("id:" + deleteResponse.getId());
        System.out.println("result:" + deleteResponse.getResult());

        client.close();

    }

    @Test
    public void matchAllQuery() {
        /**
         * 查询所有
         * 相当于 select * from article
         */
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();

        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(matchAllQueryBuilder)
                .get();

        //打印查询结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有:" + hits.getTotalHits() + "条");

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();


    }


    @Test
    public void query() {
        /**
         * 条件查询 相当于like 但是对所有字段都like
         * 相当于 select * from article where name like '%我国%' or age like '%我国%'
         */
         QueryStringQueryBuilder builder = QueryBuilders.queryStringQuery("我国");

        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(builder)
                .get();

        //打印查询结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有:" + hits.getTotalHits() + "条");

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();


    }
    @Test
    public void wildcardQuery() {

        /**
         * 通配符查询 相当于like，对指定字段进行like,不区分大小写
         * 相当于 select * from article where content like '%s%'
         *
         */
        WildcardQueryBuilder builder = QueryBuilders.wildcardQuery("content", "*s*");

        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(builder)
                .get();

        //打印查询结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有:" + hits.getTotalHits() + "条");

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }

    @Test
    public void termQuery() {
        /**
         * 常用
         * 类似于SQL中的 = 相当于= 但不是与字段等于，而是与字段分词结果等于
         * select * from article where title ='基于Lucene的搜索服务器'
         * 不区分大小写
         *
         *
         *
         *例： “我国火星探测器”
         * 分词结果 ：我，国，我国，火，星，火星，探测，探测器
         *查询：器，是查询不出来的，因为分词结果中是不包含“器”这个结果的
         *
         *
         * 如果配置了IK分词器，就使用的IK分词器，如果未配置分词器，则ES中有默认的分词器
         */
        TermQueryBuilder builder = QueryBuilders.termQuery("content", "我国火星探测器“真容”首度公开！关于取什么名字网友吵翻了");

        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(builder)
                .get();

        //打印查询结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有:" + hits.getTotalHits() + "条");

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();

    }
    @Test
    public void fuzzy(){
        /**
         * 模糊查询
         * 与termQuery 类似，但是有一些区别
         * 也是对分词结果的等于
         *
         */
        FuzzyQueryBuilder builder = QueryBuilders.fuzzyQuery("content", "我国");

        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(builder)
                .get();

        //打印查询结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有:" + hits.getTotalHits() + "条");

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();

    }

    @Test
    public void createMapping() throws ExecutionException, InterruptedException, IOException {

        // 注意：  6.X版本中 string类型换成text
        //.field("store", "yes")，中的yes或no应该为true或false
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("article")
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "text")
                                .field("store", "true")
                            .endObject()
                            .startObject("title")
                                .field("type", "text")
                                .field("store", "false")
                            .endObject()
                            .startObject("content")
                                .field("type", "text")
                                .field("store", "true")
                            .endObject()
                         .endObject()
                    .endObject()
                .endObject();

        // 2 添加mapping
        PutMappingRequest mapping = Requests.putMappingRequest("blog2").type("article").source(builder);

        client.admin().indices().putMapping(mapping).get();

        // 3 关闭资源
        client.close();

    }












}
