package soap.controller;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName: testController
 * @Description:
 * @author: 薏米滴答-西安-ZhangPY
 * @version: V1.0
 * @date: 2019/5/30 21:03
 * @Copyright: 2019 www.yimidida.com Inc. All rights reserved.
 */
@RestController
public class testController {

    @Autowired
    private RestHighLevelClient client;

    @RequestMapping(value = "add")
    public void add() {

    }

    @RequestMapping(value = "find")
    public void find() throws Exception{
        String INDEX_TEST = "booklist";

        String TYPE_TEST = "doc";

        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();

        boolBuilder.filter(QueryBuilders.matchQuery("content","深圳东路"));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(boolBuilder);
        sourceBuilder.from(0);
        // 获取记录数，默认10
        sourceBuilder.size(100);

        // 第一个是获取字段，第二个是过滤的字段，默认获取全部
        sourceBuilder.fetchSource(new String[] { "id", "title" , "content" }, new String[] {});

        SearchRequest searchRequest = new SearchRequest(INDEX_TEST);

        searchRequest.types(TYPE_TEST);
        searchRequest.source(sourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println("search: " + JSON.toJSONString(response));
        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            System.out.println("search -> " + hit.getSourceAsString());
        }

    }


}
