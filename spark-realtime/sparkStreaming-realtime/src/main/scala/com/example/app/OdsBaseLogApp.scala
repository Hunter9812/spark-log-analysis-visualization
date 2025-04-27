package com.example.app

import com.alibaba.fastjson2.{JSON, JSONArray, JSONObject, JSONWriter}
import com.example.entity.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.example.utils.{MyKafkaUtils, MyOffsetsUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 日志数据的消费分流
 * 1. 准备实时处理环境 StreamingContext
 * 2. 从Kafka中消费数据
 * 3. 处理数据
 * 3.1 转换数据结构
 * 专用结构  Bean
 * 通用结构  Map JsonObject
 * 3.2 分流
 * 4. 写出到DWD层
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    // 1. 准备实时处理环境 StreamingContext
    // NOTE: 并行度最好与 Kafka 分区数一致
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2. 从 Kafka 中消费数据
    val topicName: String = "ODS_BASE_LOG" // 与生成器配置中的主题一致
    val groupId: String = "ODS_BASE_LOG_GROUP" // 消费组ID

    //* 补充: 从 Redis 中读取 offset，指定 offset 进行消费
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = if (offsets == null || offsets.isEmpty) {
      MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    } else {
      MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    }

    //* 补充: 从当前消费到的数据中提取 offsets, 不对流中的数据做任何处理
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // 在哪里执行? driver
        rdd
      }
    )

    // 3. 处理数据
    // 3.1 转换数据结构
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        val log: String = consumerRecord.value()
        val jsonObj: JSONObject = JSON.parseObject(log)
        jsonObj
      }
    )
//    jsonObjDStream.print(1000)
    // 3.2 分流
    /*
         日志数据：
           页面访问数据
              公共字段
              页面数据
              曝光数据
              事件数据
              错误数据
           启动数据
              公共字段
              启动数据
              错误数据
    */
    val DWD_PAGE_LOG_TOPIC: String = "DWD_PAGE_LOG_TOPIC" // 页面访问
    val DWD_PAGE_DISPLAY_TOPIC: String = "DWD_PAGE_DISPLAY_TOPIC" //页面曝光
    val DWD_PAGE_ACTION_TOPIC: String = "DWD_PAGE_ACTION_TOPIC" //页面事件
    val DWD_START_LOG_TOPIC: String = "DWD_START_LOG_TOPIC" // 启动数据
    val DWD_ERROR_LOG_TOPIC: String = "DWD_ERROR_LOG_TOPIC" // 错误数据
    /*
     分流规则:
       错误数据: 不做任何的拆分，只要包含错误字段，直接整条数据发送到对应的topic
       页面数据: 拆分成 页面访问、曝光、事件 分别发送到对应的topic
       启动数据: 发动到对应的topic
    */
    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
              //. 错误数据
              val errObj: JSONObject = jsonObj.getJSONObject("err")
              if (errObj != null) {
                //将错误数据发送到 DWD_ERROR_LOG_TOPIC
                MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString())
              } else {
                // 提取公共字段
                val commonObj: JSONObject = jsonObj.getJSONObject("common")
                val ar: String = commonObj.getString("ar")
                val uid: String = commonObj.getString("uid")
                val os: String = commonObj.getString("os")
                val ch: String = commonObj.getString("ch")
                val isNew: String = commonObj.getString("is_new")
                val md: String = commonObj.getString("md")
                val mid: String = commonObj.getString("mid")
                val vc: String = commonObj.getString("vc")
                val ba: String = commonObj.getString("ba")
                // 提取时间戳
                val ts: Long = jsonObj.getLong("ts")

                //. 页面数据
                val pageObj: JSONObject = jsonObj.getJSONObject("page")
                if (pageObj != null) {
                  // 提取页面数据
                  val pageId: String = pageObj.getString("page_id")
                  val pageItem: String = pageObj.getString("item")
                  val pageItemType: String = pageObj.getString("item_type")
                  val duringTime: Long = pageObj.getLong("during_time")
                  val lastPageId: String = pageObj.getString("last_page_id")
                  val sourceType: String = pageObj.getString("source_type")
                  val pageLog: PageLog =
                    PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)
                  //发送到 DWD_PAGE_LOG_TOPIC
                  MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, JSONWriter.Feature.FieldBased))

                  // 提取曝光数据
                  val displaysJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                  if (displaysJsonArr != null && displaysJsonArr.size() > 0) {
                    for (i <- 0 until displaysJsonArr.size()) {
                      // 循环拿到每个曝光
                      val displayObj: JSONObject = displaysJsonArr.getJSONObject(i)
                      // 提取曝光字段
                      val displayType: String = displayObj.getString("display_type")
                      val displayItem: String = displayObj.getString("item")
                      val displayItemType: String = displayObj.getString("item_type")
                      val posId: String = displayObj.getString("pos_id")
                      val order: String = displayObj.getString("order")
                      // 封装成 PageDisplayLog
                      val pageDisplayLog: PageDisplayLog =
                        PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, displayType, displayItem, displayItemType, order, posId, ts)
                      // 写到 DWD_PAGE_DISPLAY_TOPIC
                      MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, JSONWriter.Feature.FieldBased))
                    }
                  }
                  // 提取事件数据
                  val actionJsonArr: JSONArray = jsonObj.getJSONArray("actions")
                  if (actionJsonArr != null && actionJsonArr.size() > 0) {
                    for (i <- 0 until actionJsonArr.size()) {
                      val actionObj: JSONObject = actionJsonArr.getJSONObject(i)
                      // 提取字段
                      val actionId: String = actionObj.getString("action_id")
                      val actionItem: String = actionObj.getString("item")
                      val actionItemType: String = actionObj.getString("item_type")
                      val actionTs: Long = actionObj.getLong("ts")

                      // 封装 PageActionLog
                      val pageActionLog: PageActionLog =
                        PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, actionId, actionItem, actionItemType, actionTs, ts)
                      // 写出到 DWD_PAGE_ACTION_TOPIC
                      MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, JSONWriter.Feature.FieldBased))
                    }
                  }
                }

                //. 启动数据
                val startJsonObj: JSONObject = jsonObj.getJSONObject("start")
                if (startJsonObj != null) {
                  // 提取字段
                  val entry: String = startJsonObj.getString("entry")
                  val loadingTime: Long = startJsonObj.getLong("loading_time")
                  val openAdId: String = startJsonObj.getString("open_ad_id")
                  val openAdMs: Long = startJsonObj.getLong("open_ad_ms")
                  val openAdSkipMs: Long = startJsonObj.getLong("open_ad_skip_ms")

                  // 封装 StartLog
                  val startLog: Any =
                    StartLog(mid, uid, ar, ch, isNew, md, os, vc, ba, entry, openAdId, loadingTime, openAdMs, openAdSkipMs, ts)
                  // 写出 DWD_START_LOG_TOPIC
                  MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, JSONWriter.Feature.FieldBased))
                }
              }
            }
            // foreachPartition里面，foreach外面: executor执行，每个分区执行一次
            MyKafkaUtils.flush()
          }
        )
/*
        rdd.foreach(
          jsonObj => {
            // 1. 在哪儿提交 offset?
            // 2. 在哪儿刷写 Kafka 缓冲区?
            // foreach里面: executor执行，每条数据执行一次, 相当于是同步发送消息.
          }
        )
*/
        // foreachRDD 里面，foreach 外面: driver执行，一批次执行一次(周期性)
        // 分流是在executor端完成，driver端做刷写，刷的不是同一个对象的缓冲区
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
        // -----------------------------------------------------------
      }
    )
    // foreachRDD 外面: driver执行，每次启动程序执行一次
    ssc.start()
    ssc.awaitTermination()
  }
}
