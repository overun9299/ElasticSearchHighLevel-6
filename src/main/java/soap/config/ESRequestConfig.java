package soap.config;

import org.elasticsearch.action.search.SearchRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

/**
 * @Description
 * @Author ZhangPY
 * @Date 2020/7/1
 */
@Configuration
public class ESRequestConfig {


    @Value("${es.indices}")
    private String indices;

    @Value("${es.types}")
    private String types;

    private SearchRequest searchRequest;

    @PostConstruct
    public void initSearchRequest() {
        SearchRequest xc_course = new SearchRequest(indices);
        xc_course.types(types);
        searchRequest = xc_course;
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }
}
