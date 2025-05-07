package com.example.app

import com.alibaba.fastjson2.{JSON, JSONObject}
import com.example.entity.{DauInfo, PageLog}
import com.example.utils.{MyBeanUtils, MyEsUtils, MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.lang
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * 日活宽表
 * 1. 准备实时环境
 * 2. 从Redis中读取偏移量
 * 3. 从kafka中消费数据
 * 4. 提取偏移量结束点
 * 5. 处理数据
 * 5.1 转换数据结构
 * 5.2 去重
 * 5.3 维度关联
 * 6. 写入ES
 * 7. 提交offsets
 * */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    // 0. 还原状态
    revertState()

    // 1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    // 2. 从redis中读取偏移量
    val topicName: String = "DWD_PAGE_LOG_TOPIC"
    val groupId: String = "DWD_DAU_GROUP"
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)
    // 3. 从Kafka中消费数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      if (offsets == null || offsets.isEmpty) {
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
    // 5.1 转换数据结构
    val pageLogDStream: DStream[PageLog] = offsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )
    //    pageLogDStream.print(100)
    // 5.2 去重
    // 自我审查: 将页面访问数据中last_page_id不为空的数据过滤掉
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )

    // 第三方审查:  通过redis将当日活跃的mid维护起来,自我审查后的每条数据需要到redis中进行比对去重
    /*
     redis中如何维护日活状态
     类型:  set
     key: DAU:DATE
     value:  mid的集合
     写入API: sadd
     读取API: smembers
     过期:  24小时
    */
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
      pageLogIter => {
        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (pageLog <- pageLogIter) {
          // 提取每条数据中的mid (我们日活的统计基于mid， 也可以基于uid)
          val mid: String = pageLog.mid

          val ts: Long = pageLog.ts
          val date = new Date(ts)
          val dateStr: String = sdf.format(date)
          val redisDauKey: String = s"DAU:$dateStr"

          val isNew: lang.Long = jedis.sadd(redisDauKey, mid)
          if (isNew == 1L) { // sadd 返回 Long 类型的值, 不加 L 会存在一次隐式类型提升(Int -> Long)
            pageLogs.append(pageLog)
          }
        }
        jedis.close()
        pageLogs.iterator
      }
    )
    //    redisFilterDStream.print()
    // 5.3 维度关联
    val dauInfoDStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(
      pageLogIter => {
        val dauInfos: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (pageLog <- pageLogIter) {
          val dauInfo = new DauInfo()
          //1. 将 pageLog 中以后的字段拷贝到DauInfo中
          MyBeanUtils.copyProperties(pageLog, dauInfo)

          // 2. 补充维度
          // 2.1  用户信息维度
          val uid: String = pageLog.user_id
          val redisUidKey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUidKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)

          val gender: String = userInfoJsonObj.getString("gender")
          val birthday: String = userInfoJsonObj.getString("birthday")
          // 2.1.1 计算年龄
          val age: Int = {
            val birthdayLd: LocalDate = LocalDate.parse(birthday)
            val nowLd: LocalDate = LocalDate.now()
            Period.between(birthdayLd, nowLd).getYears
          }
          dauInfo.user_age = age.toString
          dauInfo.user_gender = gender

          // 2.2  地区信息维度
          val provinceID: String = dauInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
          val provinceName: String = provinceJsonObj.getString("name")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          dauInfo.province_name = provinceName
          dauInfo.province_iso_code = provinceIsoCode
          dauInfo.province_3166_2 = province3166
          dauInfo.province_area_code = provinceAreaCode

          // 2.3  日期字段处理
          val date: Date = new Date(pageLog.ts)
          val dtHrArr: Array[String] = sdf.format(date).split(" ")
          val dt: String = dtHrArr(0)
          val hr: String = dtHrArr(1).split(":")(0)
          //补充到对象中
          dauInfo.dt = dt
          dauInfo.hr = hr

          dauInfos.append(dauInfo)
        }
        jedis.close()
        dauInfos.iterator
      }
    )
//    dauInfoDStream.print(100)

    // 写入到 OLAP(ES) 中
    dauInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            val docs: List[(String, DauInfo)] = dauInfoIter.map(dauInfo => (dauInfo.mid, dauInfo)).toList
            if (docs.nonEmpty) {
              val head: (String, DauInfo) = docs.head
              val ts: Long = head._2.ts
              val sdf = new SimpleDateFormat("yyyy-MM-dd")
              val dateStr: String = sdf.format(new Date(ts))
              val indexName: String = s"gmall_dau_info_$dateStr"
              // 批量写入ES
              MyEsUtils.bulkSave(indexName, docs)
            }
          }
        )
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 状态还原
   * 在每次启动实时任务时，进行一次状态还原。以 ES 为准，将所以的mid提取出来，覆盖到 Redis 中
   * */
  private def revertState(): Unit = {
    val date: LocalDate = LocalDate.now()
    val indexName: String = s"gmall_dau_info_$date"
    val fieldName: String = "mid"
    val mids: List[String] = MyEsUtils.searchField(indexName, fieldName)

    val jedis: Jedis = MyRedisUtils.getJedisFromPool()
    val redisDauKey: String = s"DAU:$date"
    jedis.del(redisDauKey)

    // 将从ES中查询到的mid覆盖到Redis中
    if (mids != null && mids.nonEmpty) {
      val pipeline: Pipeline = jedis.pipelined()
      for (mid <- mids) {
        pipeline.sadd(redisDauKey, mid) // 不会直接写入到redis中
      }
      pipeline.sync() // 执行所有的命令
    }
    jedis.close()
  }
}
