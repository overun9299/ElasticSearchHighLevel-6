package soap.test;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import soap.config.ESRequestConfig;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @Description
 * @Author ZhangPY
 * @Date 2020/7/1
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class TestSearchEVO {


    @Autowired
    RestHighLevelClient client;

    @Autowired
    RestClient restClient;

    @Autowired
    ESRequestConfig esRequestConfig;


    /**
     * MatchQuery
     * match Query即全文检索，它的搜索方式是先将搜索字符串分词，再使用各各词条从索引中搜索。
     * match query与Term query区别是match query在搜索前先将搜索关键字分词，再拿各各词语去索引中搜索。
     * @throws IOException
     * @throws ParseException
     * DSL:{"query": {"match": {"description": {"query": "spring开发","operator": "or"}}}}
     * query：搜索的关键字，对于英文关键字如果有多个单词则中间要用半角逗号分隔，而对于中文关键字中间可以用逗号分隔也可以不用。
     * operator：or 表示 只要有一个词在文档中出现则就符合条件，and表示每个词都在文档中出现则才符合条件。
     */
    @Test
    public void testMatchQuery() throws IOException, ParseException {
        /** 搜索请求对象 */
        SearchRequest searchRequest = esRequestConfig.getSearchRequest();

        /** 搜索源构建对象 */
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        /** 搜索方式,MatchQuery 由于设置了operator为or，只要有一个词匹配成功则就返回该文档。 */
        searchSourceBuilder.query(QueryBuilders.matchQuery("address","安徽三福").operator(Operator.OR));
        /** 查询id **/
        searchSourceBuilder.query(QueryBuilders.termsQuery("id","17"));

        /** 向搜索请求对象中设置搜索源 */
        searchRequest.source(searchSourceBuilder);
        /** 执行搜索,向ES发起http请求 */
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        SearchHit[] searchHits = hits.getHits();

        for(SearchHit hit:searchHits){
            String id = hit.getId();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            System.out.println(sourceAsMap);
        }

    }
}
