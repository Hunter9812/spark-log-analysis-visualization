package com.example.mapper.impl;

import com.example.entity.NameValue;
import com.example.mapper.PublisherMapper;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Consumer;

@Slf4j
@Repository
public class PublisherMapperImpl implements PublisherMapper {

    @Resource
    RestHighLevelClient esClient;

    final private String dauIndexNamePrefix = "gmall_dau_info_";
    final private String orderIndexNamePrefix = "gmall_order_wide_";

    @Override
    public Map<String, Object> searchDau(String td) {
        Map<String, Object> dauResults = new HashMap<>();
        //日活总数
        Long dauTotal = searchDauTotal(td);
        dauResults.put("dauTotal", dauTotal);

        //今日分时明细
        Map<String, Long> dauTd = searchDauHr(td);
        dauResults.put("dauTd", dauTd);

        //昨日分时明细
        //计算昨日
        LocalDate tdLd = LocalDate.parse(td);
        LocalDate ydLd = tdLd.minusDays(1);
        Map<String, Long> dauYd = searchDauHr(ydLd.toString());
        dauResults.put("dauYd", dauYd);

        return dauResults;
    }

    private SearchResponse executeSearch(String indexName, Consumer<SearchSourceBuilder> builderConsumer) {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        builderConsumer.accept(sourceBuilder);
        searchRequest.source(sourceBuilder);

        try {
            return esClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ese) {
            if (ese.status() == RestStatus.NOT_FOUND) {
                log.warn("索引不存在: {}", indexName);
                return null;
            }
            throw ese;
        } catch (IOException e) {
            throw new RuntimeException("查询索引失败", e);
        }
    }


    private Map<String, Long> searchDauHr(String td) {
        String indexName = dauIndexNamePrefix + td;
        Map<String, Long> dauHr = new HashMap<>();

        SearchResponse response = executeSearch(indexName, builder ->
                builder.size(0).aggregation(
                        AggregationBuilders
                                .terms("groupbyhr")
                                .field("hr")
                                .size(24)
                )
        );

        if (response == null) return dauHr;

        ParsedTerms parsedTerms = response.getAggregations().get("groupbyhr");
        for (Terms.Bucket bucket : parsedTerms.getBuckets()) {
            dauHr.put(bucket.getKeyAsString(), bucket.getDocCount());
        }

        return dauHr;
    }



    private Long searchDauTotal(String td) {
        String indexName = dauIndexNamePrefix + td;

        SearchResponse response = executeSearch(indexName, builder -> builder.size(0));
        if (response == null) return 0L;

        return Objects.requireNonNull(response.getHits().getTotalHits()).value;
    }


    @Override
    public List<NameValue> searchStatsByItem(String itemName, String date, String field) {
        ArrayList<NameValue> results = new ArrayList<>();

        String indexName = orderIndexNamePrefix + date;
        SearchResponse response = executeSearch(indexName, builder -> {
            builder.size(0).query(
                    QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND)
            ).aggregation(
                    AggregationBuilders
                            .terms("groupby" + field)
                            .field(field)
                            .size(100)
                            .subAggregation(
                                    AggregationBuilders
                                            .sum("totalamount")
                                            .field("split_total_amount")
                            )
            );
        });

        if (response == null) return results;

        ParsedTerms parsedTerms  = response.getAggregations().get("groupby" + field);
        List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            Aggregations bucketAggregations = bucket.getAggregations();
            ParsedSum parsedSum = bucketAggregations.get("totalamount");
            double totalamount = parsedSum.getValue();
            results.add(new NameValue(key, totalamount));
        }
        return results;
    }

    @Override
    public Map<String, Object> searchDetailByItem(String date, String itemName, Integer from, Integer pageSize) {
        HashMap<String, Object> results = new HashMap<>();

        String indexName = orderIndexNamePrefix + date;
        SearchResponse response = executeSearch(indexName, builder -> {
            builder.fetchSource(new String[]{"create_time", "order_price", "province_name", "sku_name", "sku_num", "total_amount", "user_age", "user_gender"}, null)
                    .query(QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND))
                    .from(from)
                    .size(pageSize)
                    .highlighter(new HighlightBuilder().field("sku_name"));
        });

        if (response == null) return Map.of();

        long total = Objects.requireNonNull(response.getHits().getTotalHits()).value;
        ArrayList<Map<String, Object>> sourceMaps = getMaps(response);
        results.put("total", total);
        results.put("detail", sourceMaps);

        return results;
    }

    private static ArrayList<Map<String, Object>> getMaps(SearchResponse response) {
        SearchHit[] searchHits = response.getHits().getHits();
        ArrayList<Map<String, Object>> sourceMaps = new ArrayList<>();
        for (SearchHit searchHit : searchHits) {
            Map<String, Object> sourceMap = searchHit.getSourceAsMap();
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            Text[] fragments = highlightFields.get("sku_name").getFragments();
            String highLightSkuName = fragments[0].toString();
            // 使用高亮字段替换原字段
            sourceMap.put("sku_name", highLightSkuName);

            sourceMaps.add(sourceMap);
        }
        return sourceMaps;
    }

}
