package soap.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @ClassName: EsConfiguration
 * @Description: 配置es
 * @author: 薏米滴答-西安-zhangPY
 * @version: V1.0
 * @date: 2019/4/18 9:55
 * @Copyright: 2019 www.yimidida.com Inc. All rights reserved.
 */

@Configuration
public class EsConfiguration {

    @Value("${overun.elasticsearch.hostlist}")
    private String hostlist;


    /**
     * 创建RestHighLevelClient客户端
     * @return
     */
    @Bean
    public RestHighLevelClient restHighLevelClient() {
        /** 解析hostlist配置信息 */
        String[] split = hostlist.split(",");
        /** 创建HttpHost数组，其中存放es主机和端口的配置信息 */
        HttpHost[] httpHostArray = new HttpHost[split.length];
        for(int i=0;i<split.length;i++){
            String item = split[i];
            httpHostArray[i] = new HttpHost(item.split(":")[0], Integer.parseInt(item.split(":")[1]), "http");
        }
        /** 返回RestHighLevelClient客户端 */
        return new RestHighLevelClient(RestClient.builder(httpHostArray));
    }


    /**
     * 项目主要使用RestHighLevelClient，对于低级的客户端暂时不用
     * @return
     */
    @Bean
    public RestClient restClient() {
        /** 解析hostlist配置信息 */
        String[] split = hostlist.split(",");
        /** 创建HttpHost数组，其中存放es主机和端口的配置信息 */
        HttpHost[] httpHostArray = new HttpHost[split.length];
        for(int i=0; i<split.length; i++){
            String item = split[i];
            httpHostArray[i] = new HttpHost(item.split(":")[0], Integer.parseInt(item.split(":")[1]), "http");
        }
        return RestClient.builder(httpHostArray).build();
    }


}
