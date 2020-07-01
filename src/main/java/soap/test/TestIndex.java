package soap.test;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ZhangPY on 2019/8/11
 * Belong Organization OVERUN-9299
 * overun9299@163.com
 * Explain: 创建es索引
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class TestIndex {

    @Autowired
    RestHighLevelClient client;

    @Autowired
    RestClient restClient;

    /**
     * 创建索引库
     * @throws IOException
     */
    @Test
    public void testCreateIndex() throws IOException {
        String sourceOne = "{\n" +
                " \t\"properties\": {\n" +
                "            \"studymodel\":{\n" +
                "             \"type\":\"keyword\"\n" +
                "           },\n" +
                "            \"name\":{\n" +
                "             \"type\":\"keyword\"\n" +
                "           },\n" +
                "           \"description\": {\n" +
                "              \"type\": \"text\",\n" +
                "              \"analyzer\":\"ik_max_word\",\n" +
                "              \"search_analyzer\":\"ik_smart\"\n" +
                "           },\n" +
                "           \"pic\":{\n" +
                "             \"type\":\"text\",\n" +
                "             \"index\":false\n" +
                "           }\n" +
                " \t}\n" +
                "}";

        String sourceTwo = "{\n" +
                " \t\"properties\": {\n" +
                "            \"id\":{\n" +
                "             \"type\":\"keyword\"\n" +
                "           },\n" +
                "            \"fname\":{\n" +
                "             \"type\":\"keyword\"\n" +
                "           },\n" +
                "           \"lname\":{\n" +
                "             \"type\":\"keyword\"\n" +
                "           },\n" +
                "           \"age\":{\n" +
                "             \"type\":\"keyword\"\n" +
                "           },\n" +
                "           \"sex\":{\n" +
                "             \"type\":\"keyword\"\n" +
                "           },\n" +
                "           \"d_id\":{\n" +
                "             \"type\":\"keyword\"\n" +
                "           },\n" +
                "           \"address\": {\n" +
                "              \"type\": \"text\",\n" +
                "              \"analyzer\":\"ik_max_word\",\n" +
                "              \"search_analyzer\":\"ik_smart\"\n" +
                "           },\n" +
                "           \"describes\": {\n" +
                "              \"type\": \"text\",\n" +
                "              \"analyzer\":\"ik_max_word\",\n" +
                "              \"search_analyzer\":\"ik_smart\"\n" +
                "           }\n" +
                " \t}\n" +
                "}";


        /** 创建索引对象 */
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("xc_course");
        /** 设置参数 number_of_replicas 是数据备份数，如果只有一台机器，设置为0 、number_of_shards  是数据分片数，默认为5，有时候设置为3 */
        createIndexRequest.settings(Settings.builder().put("number_of_shards","1").put("number_of_replicas","0"));
        /** 指定映射 */
        createIndexRequest.mapping("doc",sourceTwo, XContentType.JSON);
        /** 操作索引的客户端 */
        IndicesClient indices = client.indices();
        /** 执行创建索引库 */
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest);
        /** 得到响应 */
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println(acknowledged);

    }

    /**
     * 删除索引库
     * @throws IOException
     */
    @Test
    public void testDeleteIndex() throws IOException {
        /** 删除索引对象 */
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("xc_course");
        /** 操作索引的客户端 */
        IndicesClient indices = client.indices();
        /** 执行删除索引 */
        DeleteIndexResponse delete = indices.delete(deleteIndexRequest);
        /** 得到响应 */
        boolean acknowledged = delete.isAcknowledged();
        System.out.println(acknowledged);

    }

    /**
     * 添加文档
     * @throws IOException
     */
    @Test
    public void testAddDoc() throws IOException {
        /** 文档内容 、 准备json数据 */
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "spring cloud实战");
        jsonMap.put("description", "本课程主要从四个章节进行讲解： 1.微服务架构入门 2.spring cloud 基础入门 3.实战Spring Boot 4.注册中心eureka。");
        jsonMap.put("studymodel", "201001");
        SimpleDateFormat dateFormat =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        jsonMap.put("timestamp", dateFormat.format(new Date()));
        jsonMap.put("price", 5.6f);

        /** 测试数据 **/
        Map<String, Object> jsonMapTest = new HashMap<>();
        jsonMapTest.put("id","88");
        jsonMapTest.put("fname","tU09zd");
        jsonMapTest.put("lname","kP");
        jsonMapTest.put("age","39");
        jsonMapTest.put("sex","1");
        jsonMapTest.put("d_id","120189");
        jsonMapTest.put("address","岛安上浙安徽西杭津深安北江蒙岛温内州圳徽三");
        jsonMapTest.put("describes","善老斯贤忠惠勇劳实实勤热敢良文热");

        /** 创建索引创建对象 可设置es主键 _id */
        IndexRequest indexRequest = new IndexRequest("xc_course","doc", "17");
        /** 文档内容 */
        indexRequest.source(jsonMapTest);
        /** 通过client进行http的请求 */
        IndexResponse indexResponse = client.index(indexRequest);
        DocWriteResponse.Result result = indexResponse.getResult();
        System.out.println(result);

    }


    @Test
    public void batchInsert() throws IOException {
        List<Map<String,Object>> mapList = new ArrayList<>();
        String type = "doc";

        Map<String, Object> jsonMapTestOne = new HashMap<>();
        jsonMapTestOne.put("id","1");
        jsonMapTestOne.put("fname","tU09zd");
        jsonMapTestOne.put("lname","kP");
        jsonMapTestOne.put("age","39");
        jsonMapTestOne.put("sex","1");
        jsonMapTestOne.put("d_id","120189");
        jsonMapTestOne.put("address","岛安上浙安徽西杭津深安北江蒙岛温内州圳徽三");
        jsonMapTestOne.put("describes","善老斯贤忠惠勇劳实实勤热敢良文热");

        Map<String, Object> jsonMapTest = new HashMap<>();
        jsonMapTest.put("id","2");
        jsonMapTest.put("fname","tDAQkVtq");
        jsonMapTest.put("lname","fvAgp3VkFd");
        jsonMapTest.put("age","1");
        jsonMapTest.put("sex","1");
        jsonMapTest.put("d_id","785391");
        jsonMapTest.put("address","深深青三福海都庆岛岛福浙三深北蒙三州徽");
        jsonMapTest.put("describes","惠客勤好热厚好惠善实好忠热");

        mapList.add(jsonMapTestOne);
        mapList.add(jsonMapTest);
        mapList.add(jsonMapTest);


        BulkRequest request = new BulkRequest();

        for (Map<String,Object> map : mapList) {
            IndexRequest indexRequest= new IndexRequest("xc_course", type, map.get("id").toString()).source(map);
            request.add(indexRequest);
        }

        BulkResponse bulkItemResponses = client.bulk(request);

        for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
            if (bulkItemResponse.isFailed()) {
                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                System.out.println("批量插入失败"+failure.getStatus());
            }
        }
    }

    /**
     * 查询文档
     * @throws IOException
     */
    @Test
    public void testGetDoc() throws IOException {
        /** 查询请求对象 */
        GetRequest getRequest = new GetRequest("xc_course","doc","1");
        GetResponse getResponse = client.get(getRequest);
        /** 得到文档的内容 */
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        System.out.println(sourceAsMap);
    }
}
