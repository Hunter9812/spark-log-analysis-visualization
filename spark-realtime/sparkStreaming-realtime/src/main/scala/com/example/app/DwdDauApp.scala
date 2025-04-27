package com.example.app

import com.alibaba.fastjson2.JSON
import com.example.entity.PageLog
import com.example.utils.{MyKafkaUtils, MyOffsetsUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 日活宽表
 * 1. 准备实时环境
 * 2. 从Redis中读取偏移量
 * 3. 从kafka中消费数据
 * 4. 提取偏移量结束点
 * 5. 处理数据
 *     5.1 转换数据结构
 *     5.2 去重
 *     5.3 维度关联
 * 6. 写入ES
 * 7. 提交offsets
 * */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    // 1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
    val topicName : String = "DWD_PAGE_LOG_TOPIC"
    val groupId : String = "DWD_DAU_GROUP"
    // 2. 从redis中读取偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)
    // 3. 从Kafka中消费数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = if (offsets == null || offsets.isEmpty) {
      MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    } else {
      MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    }
    // 4. 提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    // 5. 处理数据
    val pageLogDStream: DStream[PageLog] = offsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )
    pageLogDStream.print(100)
    
    ssc.start()
    ssc.awaitTermination()
  }
}
