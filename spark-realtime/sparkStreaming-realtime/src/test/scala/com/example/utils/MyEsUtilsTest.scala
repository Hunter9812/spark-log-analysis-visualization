package com.example.utils

import org.elasticsearch.client.RestHighLevelClient
import org.junit.jupiter.api.Test;

class MyEsUtilsTest {
  @Test
  def testEsClient(): Unit = {
    val esClient: RestHighLevelClient = MyEsUtils.esClient
    println(esClient)
    MyEsUtils.close()
  }

  @Test
  def searchField(): Unit = {
    val list: List[String] = MyEsUtils.searchField("gmall_dau_info_all", "mid")
    println(list.size)
    println(list)
  }
}
