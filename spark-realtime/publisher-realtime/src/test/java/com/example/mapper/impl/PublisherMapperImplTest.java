package com.example.mapper.impl;

import com.example.entity.NameValue;
import jakarta.annotation.Resource;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@SpringBootTest
class PublisherMapperImplTest {

    @Resource
    PublisherMapperImpl publisherMapper;

    @Test
    void testConnectEs() throws IOException {
        RestHighLevelClient esClient = publisherMapper.esClient;
        if (esClient != null) {
            System.out.println("Elasticsearch client is connected successfully.");
            esClient.close();
        } else {
            System.out.println("Failed to connect to Elasticsearch.");
        }
    }

    @Test
    void searchDau() {
        String td = "2025-05-04";
        Map<String, Object> map = publisherMapper.searchDau(td);
        map.forEach(
                (k, v) -> {
                    System.out.println(k + " : " + v);
                }
        );
    }

    @Test
    void searchStatsByItem() {
        List<NameValue> list = publisherMapper.searchStatsByItem("小米手机", "2025-05-04", "user_age");
        for (NameValue value : list) {
            System.out.println(value.getName() + " : " + value.getValue());
        }
    }
}