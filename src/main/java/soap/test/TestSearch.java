package soap.test;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
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
 * Created by ZhangPY on 2019/8/11
 * Belong Organization OVERUN-9299
 * overun9299@163.com
 * Explain: 使用RestClient查询es
 * RestClient: 是官方推荐使用的，它包括两种：Java Low Level REST Client和 Java High Level REST Client。
 */

@SpringBootTest
@RunWith(SpringRunner.class)
public class TestSearch {

    @Autowired
    RestHighLevelClient client;

    @Autowired
    RestClient restClient;

    @Autowired
    ESRequestConfig esRequestConfig;

    /** DSL搜索 : DSL(Domain Specific Language)是ES提出的基于json的搜索方式，在搜索时传入特定的json格式的数据来完成不同的搜索需求。*/
    /** 查询指定索引库指定类型下的文档。（通过使用此方法） 发送：post http://localhost:9200/xc_course(索引名)/doc(文档类型创建时已确定)/_search */

    /**
     * 搜索全部记录
     * @throws IOException
     * @throws ParseException
     * DSL: {"query": {"match_all": {}},"_source": ["name","studymodel"]}
     * _source：source源过虑设置，指定结果中所包括的字段有哪些。
     */
    @Test
    public void testSearchAll() throws IOException, ParseException {
        /** 搜索请求对象 */
        SearchRequest searchRequest = new SearchRequest("xc_course");
        /** 指定类型 */
        searchRequest.types("doc");
        /** 搜索源构建对象 */
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        /** 搜索方式 、matchAllQuery搜索全部 */
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        /** 设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段 */
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});
        /** 向搜索请求对象中设置搜索源 */
        searchRequest.source(searchSourceBuilder);
        /** 执行搜索,向ES发起http请求 */
        SearchResponse searchResponse = client.search(searchRequest);
        /** 搜索结果 */
        SearchHits hits = searchResponse.getHits();
        /** 匹配到的总记录数 此处totalHits如果没有使用分页则totalHits=searchHits.length,反之则有可能不同 */
        long totalHits = hits.getTotalHits();
        /** 得到匹配度高的文档 */
        SearchHit[] searchHits = hits.getHits();
        /** 日期格式化对象 */
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(SearchHit hit:searchHits){
            /** 文档的主键 */
            String id = hit.getId();
            /** 源文档内容 */
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            /** 由于前边设置了源文档字段过虑，这时description是取不到的 */
            String description = (String) sourceAsMap.get("description");
            /** 学习模式 */
            String studymodel = (String) sourceAsMap.get("studymodel");
            /** 价格 */
            Double price = (Double) sourceAsMap.get("price");
            /** 日期 */
            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }

    }

    /**
     * 分页查询
     * @throws IOException
     * @throws ParseException
     * DSL:{"from": 0,"size": 1,"query": {"match_all": {}},"_source": ["name","studymodel"]}
     * form：表示起始文档的下标，从0开始。 size：查询的文档数量。
     */
    @Test
    public void testSearchPage() throws IOException, ParseException {
        /** 搜索请求对象 */
        SearchRequest searchRequest = new SearchRequest("xc_course");
        /** 指定类型 */
        searchRequest.types("doc");
        /** 搜索源构建对象 */
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        /** 设置分页参数 */
        /** 页码 */
        int page = 1;
        /** 每页记录数 */
        int size = 1;
        /** 计算出记录起始下标 */
        int from  = (page-1)*size;
        /** 起始记录下标，从0开始 */
        searchSourceBuilder.from(from);
        /** 每页显示的记录数 */
        searchSourceBuilder.size(size);
        /** 搜索方式 、 matchAllQuery搜索全部  */
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        /** 设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段 */
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});
        /** 向搜索请求对象中设置搜索源 */
        searchRequest.source(searchSourceBuilder);
        /** 执行搜索,向ES发起http请求 */
        SearchResponse searchResponse = client.search(searchRequest);
        /** 搜索结果 */
        SearchHits hits = searchResponse.getHits();
        /** 匹配到的总记录数 此处totalHits如果没有使用分页则totalHits=searchHits.length,反之则有可能不同 */
        long totalHits = hits.getTotalHits();
        /** 得到匹配度高的文档 */
        SearchHit[] searchHits = hits.getHits();
        /** 日期格式化对象 */
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(SearchHit hit:searchHits){
            /** 文档的主键 */
            String id = hit.getId();
            /** 源文档内容 */
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            /** 由于前边设置了源文档字段过虑，这时description是取不到的 */
            String description = (String) sourceAsMap.get("description");
            /** 学习模式 */
            String studymodel = (String) sourceAsMap.get("studymodel");
            /** 价格 */
            Double price = (Double) sourceAsMap.get("price");
            /** 日期 */
            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }

    }


    /**
     * 精确查询
     * Term Query为精确查询，在搜索时会整体匹配关键字，不再将关键字分词。
     * @throws IOException
     * @throws ParseException
     * DSL:{"query": {"term": {"name": "spring"}},"_source": ["name","studymodel"]}
     */
    @Test
    public void testTermQuery() throws IOException, ParseException {
        /** 搜索请求对象 */
        SearchRequest searchRequest = new SearchRequest("xc_course");
        /** 指定类型 */
        searchRequest.types("doc");
        /** 搜索源构建对象 */
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        /** 设置分页参数 、 页码 */
        int page = 1;
        /** 每页记录数 */
        int size = 1;
        /** 计算出记录起始下标 */
        int from  = (page-1)*size;
        /** 起始记录下标，从0开始 */
        searchSourceBuilder.from(from);
        /** 每页显示的记录数 */
        searchSourceBuilder.size(size);
        /** 搜索方式 、termQuery */
        searchSourceBuilder.query(QueryBuilders.termQuery("name","spring"));
        /** 设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段 */
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});
        /** 向搜索请求对象中设置搜索源 */
        searchRequest.source(searchSourceBuilder);
        /** 执行搜索,向ES发起http请求 */
        SearchResponse searchResponse = client.search(searchRequest);
        /** 搜索结果 */
        SearchHits hits = searchResponse.getHits();
        /** 匹配到的总记录数 */
        long totalHits = hits.getTotalHits();
        /** 得到匹配度高的文档 */
        SearchHit[] searchHits = hits.getHits();
        /** 日期格式化对象 */
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(SearchHit hit:searchHits){
            /** 文档的主键 */
            String id = hit.getId();
            /** 源文档内容 */
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            /** 由于前边设置了源文档字段过虑，这时description是取不到的 */
            String description = (String) sourceAsMap.get("description");
            /** 学习模式 */
            String studymodel = (String) sourceAsMap.get("studymodel");
            /** 价格 */
            Double price = (Double) sourceAsMap.get("price");
            /** 日期 */
            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }

    }

    /**
     * 根据id查询
     * @throws IOException
     * @throws ParseException
     * DSL:{"query": {"ids": {"type": "doc","values": ["3","4","100"]}}}
     * ES提供根据多个id值匹配的方法
     */
    @Test
    public void testTermQueryByIds() throws IOException, ParseException {
        /** 搜索请求对象 */
        SearchRequest searchRequest = new SearchRequest("xc_course");
        /** 指定类型 */
        searchRequest.types("doc");
        /** 搜索源构建对象 */
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        /** 根据id查询 ，定义id */
        String[] ids = new String[]{"1","2"};
        /** 此处注意为termsQuery */
        searchSourceBuilder.query(QueryBuilders.termsQuery("_id",ids));
        /** 设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段 */
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});
        /** 向搜索请求对象中设置搜索源 */
        searchRequest.source(searchSourceBuilder);
        /** 执行搜索,向ES发起http请求 */
        SearchResponse searchResponse = client.search(searchRequest);
        /** 搜索结果 */
        SearchHits hits = searchResponse.getHits();
        /** 匹配到的总记录数 */
        long totalHits = hits.getTotalHits();
        /** 得到匹配度高的文档 */
        SearchHit[] searchHits = hits.getHits();
        /** 日期格式化对象 */
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(SearchHit hit:searchHits){
            /** 文档的主键 */
            String id = hit.getId();
            /** 源文档内容 */
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            /** 由于前边设置了源文档字段过虑，这时description是取不到的 */
            String description = (String) sourceAsMap.get("description");
            /** 学习模式 */
            String studymodel = (String) sourceAsMap.get("studymodel");
            /** 价格 */
            Double price = (Double) sourceAsMap.get("price");
            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }

    }


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
        SearchRequest searchRequest = new SearchRequest("xc_course");
        /** 指定类型 */
        searchRequest.types("doc");
        /** 搜索源构建对象 */
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        /** 搜索方式,MatchQuery 由于设置了operator为or，只要有一个词匹配成功则就返回该文档。 */
//        searchSourceBuilder.query(QueryBuilders.matchQuery("description","spring开发框架").minimumShouldMatch("80%"));
        searchSourceBuilder.query(QueryBuilders.matchQuery("description","spring开发框架").operator(Operator.OR));
        /** 设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段 */
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});
        /** 向搜索请求对象中设置搜索源 */
        searchRequest.source(searchSourceBuilder);
        /** 执行搜索,向ES发起http请求 */
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        SearchHit[] searchHits = hits.getHits();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(SearchHit hit:searchHits){
            String id = hit.getId();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String description = (String) sourceAsMap.get("description");
            String studymodel = (String) sourceAsMap.get("studymodel");
            Double price = (Double) sourceAsMap.get("price");
            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }

    }


    /**
     * minimum_should_match
     * 上边使用的operator = or表示只要有一个词匹配上就得分，如果实现三个词至少有两个词匹配如何实现？
     * 使用minimum_should_match可以指定文档匹配词的占比
     * @throws IOException
     * @throws ParseException
     * DSL:{"query": {"match": {"description": {"query": "spring开发框架","minimum_should_match": "80%"}}}}
     * 设置"minimum_should_match": "80%"表示，三个词在文档的匹配占比为80%，即3*0.8=2.4，向上取整得2，表示至少有两个词在文档中要匹配成功
     */
    @Test
    public void testShouldMatchQuery() throws IOException, ParseException {
        /** 搜索请求对象 */
        SearchRequest searchRequest = new SearchRequest("xc_course");
        /** 指定类型 */
        searchRequest.types("doc");
        /** 搜索源构建对象 */
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        /** 搜索方式,MatchQuery 由于设置了operator为or，只要有一个词匹配成功则就返回该文档。 */
        searchSourceBuilder.query(QueryBuilders.matchQuery("description","spring开发框架").minimumShouldMatch("80%"));
        /** 设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段 */
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});
        /** 向搜索请求对象中设置搜索源 */
        searchRequest.source(searchSourceBuilder);
        /** 执行搜索,向ES发起http请求 */
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        SearchHit[] searchHits = hits.getHits();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(SearchHit hit:searchHits){
            String id = hit.getId();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");
            String description = (String) sourceAsMap.get("description");
            String studymodel = (String) sourceAsMap.get("studymodel");
            Double price = (Double) sourceAsMap.get("price");
            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }

    }

    /**
     * MultiMatchQuery(包括提升boost权重)
     * 上边学习的termQuery和matchQuery一次只能匹配一个Field，本节学习multiQuery，一次可以匹配多个字段。
     * @throws IOException
     * @throws ParseException
     * DSL:{"query": {"multi_match": {"query": "spring css","minimum_should_match": "50%","fields": ["name","description"]}}}
     *
     * DSL:{"query": {"multi_match": {"query": "spring框架","minimum_should_match": "50%","fields": ["name^10","description"]}}}
     * 提升boost，通常关键字匹配上name的权重要比匹配上description的权重高，这里可以对name的权重提升。
     * "name^10" 表示权重提升10倍
     */
    @Test
    public void testMultiMatchQuery() throws IOException, ParseException {

        SearchRequest searchRequest = new SearchRequest("xc_course");

        searchRequest.types("doc");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        /** MultiMatchQuery 包括提升name的权重 */
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("spring css","name","description").minimumShouldMatch("50%").field("name",10));

        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);

        SearchHits hits = searchResponse.getHits();

        long totalHits = hits.getTotalHits();

        SearchHit[] searchHits = hits.getHits();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(SearchHit hit:searchHits){

            String id = hit.getId();

            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");

            String description = (String) sourceAsMap.get("description");

            String studymodel = (String) sourceAsMap.get("studymodel");

            Double price = (Double) sourceAsMap.get("price");

            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }

    }


    /**
     * BoolQuery
     * 布尔查询对应于Lucene的BooleanQuery查询，实现将多个查询组合起来
     * @throws IOException
     * @throws ParseException
     * DSL:{"_source": ["name","studymodel","description"],"from": 0,"size": 1,"query": {"bool": {"must": [{"multi_match": {"query": "spring框架","minimum_should_match": "50%","fields": ["name^10","description"]}},{"term": {"studymodel": "201001"}}]}}}
     * must：表示必须，多个查询条件必须都满足。（通常使用must）
     * should：表示或者，多个查询条件只要有一个满足即可。
     * must_not：表示非。
     */
    @Test
    public void testBoolQuery() throws IOException, ParseException {
        /** 搜索请求对象 */
        SearchRequest searchRequest = new SearchRequest("xc_course");
        /** 指定类型 */
        searchRequest.types("doc");
        /** 搜索源构建对象 */
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        /** boolQuery搜索方式，先定义一个MultiMatchQuery */
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("spring css", "name", "description")
                .minimumShouldMatch("50%")
                .field("name", 10);
        /** 再定义一个termQuery */
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("studymodel", "201001");

        /** 定义一个boolQuery */
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        boolQueryBuilder.must(termQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);
        /** 设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段 */
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});
        /** 向搜索请求对象中设置搜索源 */
        searchRequest.source(searchSourceBuilder);
        /** 执行搜索,向ES发起http请求 */
        SearchResponse searchResponse = client.search(searchRequest);
        /** 搜索结果 */
        SearchHits hits = searchResponse.getHits();
        /** 匹配到的总记录数 */
        long totalHits = hits.getTotalHits();
        /** 得到匹配度高的文档 */
        SearchHit[] searchHits = hits.getHits();
        /** 日期格式化对象 */
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(SearchHit hit:searchHits){

            String id = hit.getId();

            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");

            String description = (String) sourceAsMap.get("description");

            String studymodel = (String) sourceAsMap.get("studymodel");

            Double price = (Double) sourceAsMap.get("price");

            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }

    }

    /**
     * 过虑器查询
     * 过虑是针对搜索的结果进行过虑，过虑器主要判断的是文档是否匹配，不去计算和判断文档的匹配度得分，所以过虑器性能比查询要高，且方便缓存，推荐尽量使用过虑器去实现查询或者过虑器和查询共同使用
     * 过虑器在布尔查询中使用，下边是在搜索结果的基础上进行过虑
     * @throws IOException
     * @throws ParseException
     * DSL:{"_source": ["name","studymodel","description","price"],"query": {"bool": {"must": [{"multi_match": {"query": "spring框架","minimum_should_match": "50%","fields": ["name^10","description"]}}],"filter": [{"term": {"studymodel": "201001"}},{"range": {"price": {"gte": 60,"lte": 100}}}]}}}
     * range：范围过虑，保留大于等于60 并且小于等于100的记录。
     * term：项匹配过虑，保留studymodel等于"201001"的记录。
     * range和term一次只能对一个Field设置范围过虑。
     */
    @Test
    public void testFilter() throws IOException, ParseException {
        /** 搜索请求对象 */
        SearchRequest searchRequest = new SearchRequest("xc_course");
        /** 指定类型 */
        searchRequest.types("doc");
        /** 搜索源构建对象 */
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        /** boolQuery搜索方式，先定义一个MultiMatchQuery */
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("spring css", "name", "description")
                .minimumShouldMatch("50%")
                .field("name", 10);

        /** 定义一个boolQuery */
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);
        /** 定义过虑器 */
        boolQueryBuilder.filter(QueryBuilders.termQuery("studymodel","201001"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(90).lte(100));

        searchSourceBuilder.query(boolQueryBuilder);
        /** 设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段 */
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);

        SearchHits hits = searchResponse.getHits();

        long totalHits = hits.getTotalHits();

        SearchHit[] searchHits = hits.getHits();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(SearchHit hit:searchHits){

            String id = hit.getId();

            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");

            String description = (String) sourceAsMap.get("description");

            String studymodel = (String) sourceAsMap.get("studymodel");

            Double price = (Double) sourceAsMap.get("price");

            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }

    }

    /**
     * Sort
     * 可以在字段上添加一个或多个排序，支持在keyword、date、float等类型上添加，text类型的字段上不允许添加排序。
     * @throws IOException
     * @throws ParseException
     * DSL:{"_source": ["name","studymodel","description","price"],"query": {"bool": {"filter": [{"range": {"price": {"gte": 0,"lte": 100}}}]}},"sort": [{"studymodel": "desc"},{"price": "asc"}]}
     *
     */
    @Test
    public void testSort() throws IOException, ParseException {
        /** 搜索请求对象 */
        SearchRequest searchRequest = new SearchRequest("xc_course");
        /** 指定类型 */
        searchRequest.types("doc");
        /** 搜索源构建对象 */
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        /** boolQuery搜索方式，定义一个boolQuery */
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        /** 定义过虑器 */
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(0).lte(100));

        searchSourceBuilder.query(boolQueryBuilder);
        /** 添加排序 */
        searchSourceBuilder.sort("studymodel", SortOrder.DESC);
        searchSourceBuilder.sort("price", SortOrder.ASC);
        /** 设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段 */
        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});
        /** 向搜索请求对象中设置搜索源 */
        searchRequest.source(searchSourceBuilder);
        /** 执行搜索,向ES发起http请求 */
        SearchResponse searchResponse = client.search(searchRequest);
        /** 搜索结果 */
        SearchHits hits = searchResponse.getHits();
        /** 匹配到的总记录数 */
        long totalHits = hits.getTotalHits();

        SearchHit[] searchHits = hits.getHits();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(SearchHit hit:searchHits){

            String id = hit.getId();

            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("name");

            String description = (String) sourceAsMap.get("description");

            String studymodel = (String) sourceAsMap.get("studymodel");

            Double price = (Double) sourceAsMap.get("price");

            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }

    }



    /**
     * Highlight
     * 高亮显示可以将搜索结果一个或多个字突出显示，以便向用户展示匹配关键字的位置
     * @throws IOException
     * @throws ParseException
     * DSL:{"_source" : [ "name", "studymodel", "description","price"],"query": {"bool" : {"must":[{"multi_match" : {"query" : "开发框架","minimum_should_match": "50%","fields": [ "name^10", "description" ],"type":"best_fields"}}],"filter": [{ "range": { "price": { "gte": 0 ,"lte" : 100}}}]}},"sort" : [{"price" : "asc"}],"highlight": {"pre_tags": ["<tag1>"],"post_tags": ["</tag2>"],"fields": {"name": {},"description":{}}}}
     */
    @Test
    public void testHighlight() throws IOException, ParseException {

        SearchRequest searchRequest = new SearchRequest("xc_course");

        searchRequest.types("doc");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        /** boolQuery搜索方式,先定义一个MultiMatchQuery */
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("开发框架", "name", "description")
                .minimumShouldMatch("50%")
                .field("name", 10);


        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(multiMatchQueryBuilder);

        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(0).lte(100));

        searchSourceBuilder.query(boolQueryBuilder);

        searchSourceBuilder.fetchSource(new String[]{"name","studymodel","price","timestamp"},new String[]{});

        /** 设置高亮 */
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<tag>");
        highlightBuilder.postTags("</tag>");
        /** 对name加高亮 */
        highlightBuilder.fields().add(new HighlightBuilder.Field("name"));
//        highlightBuilder.fields().add(new HighlightBuilder.Field("description"));
        searchSourceBuilder.highlighter(highlightBuilder);


        searchRequest.source(searchSourceBuilder);
        /** 执行搜索,向ES发起http请求 */
        SearchResponse searchResponse = client.search(searchRequest);
        /** 搜索结果 */
        SearchHits hits = searchResponse.getHits();
        /** 匹配到的总记录数 */
        long totalHits = hits.getTotalHits();
        /** 得到匹配度高的文档 */
        SearchHit[] searchHits = hits.getHits();
        /** 日期格式化对象 */
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(SearchHit hit:searchHits){
            /** 文档的主键 */
            String id = hit.getId();
            /** 源文档内容 */
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            /** 源文档的name字段内容 */
            String name = (String) sourceAsMap.get("name");
            /** 取出高亮字段 */
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if(highlightFields!=null){
                /** 取出name高亮字段 */
                HighlightField nameHighlightField = highlightFields.get("name");
                if(nameHighlightField!=null){
                    /** 去除内容，此处为一段一段的，所以后面要拼接 */
                    Text[] fragments = nameHighlightField.getFragments();
                    StringBuffer stringBuffer = new StringBuffer();
                    for(Text text:fragments){
                        stringBuffer.append(text);
                    }
                    name = stringBuffer.toString();
                }
            }

            /** 由于前边设置了源文档字段过虑，这时description是取不到的 */
            String description = (String) sourceAsMap.get("description");

            String studymodel = (String) sourceAsMap.get("studymodel");

            Double price = (Double) sourceAsMap.get("price");

            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
            System.out.println(studymodel);
            System.out.println(description);
        }

    }
}
