package com.example.utils

import com.alibaba.fastjson2.{JSON, JSONWriter}
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClientBuilder}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.xcontent.XContentType

import java.util
import scala.collection.mutable.ListBuffer

/**
 * ES 工具类，用于封装一些常用的操作
 * */
object MyEsUtils {
  val esClient = {
    val host: String = MyPropsUtils(Const.ES_HOST)
    val port: String = MyPropsUtils(Const.ES_PORT)
    val restClient: RestClient = RestClient.builder(
      new HttpHost(host, port.toInt)
    ).build()
    new RestHighLevelClientBuilder(restClient)
      .setApiCompatibilityMode(true)
      .build()
  }

  def close(): Unit = {
    if (esClient != null) {
      esClient.close()
    }
  }

  /**
   * 1. 批量写入
   * 2. 幂等写入
   * */
  def bulkSave(indexName: String, docs: List[(String, AnyRef)]) = {
    val bulkRequest = new BulkRequest(indexName)
    for ((docId, docObj) <- docs) {
      val indexRequest = new IndexRequest()
      val dataJson: String = JSON.toJSONString(docObj, JSONWriter.Feature.FieldBased)
      indexRequest.source(dataJson, XContentType.JSON)
      indexRequest.id(docId)
      bulkRequest.add(indexRequest)
    }
    esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

  /**
   * 查询指定的字段
   * */
  def searchField(indexName: String, fieldName: String): List[String] = {
    val getIndexRequest = new GetIndexRequest(indexName)
    val isExists: Boolean = esClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT)
    if (!isExists) {
      return null
    }
    val mids: ListBuffer[String] = ListBuffer[String]()
    val searchRequest = new SearchRequest(indexName)
    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.fetchField(fieldName).size(100000)
    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      val sourceAsMap: util.Map[String, AnyRef] = hit.getSourceAsMap
      val mid: String = sourceAsMap.get(fieldName).toString
      mids.append(mid)
    }
    mids.toList
  }
}
