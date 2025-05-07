package com.example.app

import com.alibaba.fastjson2.{JSON, JSONObject, JSONWriter}
import com.example.entity.{OrderDetail, OrderInfo, OrderWide}
import com.example.utils.{MyEsUtils, MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.time.{LocalDate, Period}
import java.util
import scala.collection.mutable.ListBuffer

/**
 * 订单宽表任务
 *
 * 1. 准备实时环境
 * 2. 从 Redis 中读取 offset  * 2
 * 3. 从 kafka 中消费数据 * 2
 * 4. 提取offset * 2
 * 5. 数据处理
 * 5.1 转换结构
 * 5.2 维度关联
 * 5.3 双流join
 * 6. 写入ES
 * 7. 提交offset * 2
 */
object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_order_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoTopicName: String = "DWD_ORDER_INFO_I"
    val orderInfoGroupId: String = "DWD_ORDER_INFO:GROUP"
    val orderInfoOffsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(orderInfoTopicName, orderInfoGroupId)
    val orderDetailTopicName: String = "DWD_ORDER_DETAIL_I"
    val orderDetailGroupId: String = "DWD_ORDER_DETAIL:GROUP"
    val orderDetailOffsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(orderDetailTopicName, orderDetailGroupId)

    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      if (orderInfoOffsets == null || orderInfoOffsets.isEmpty) {
        MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroupId)
      } else {
        MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroupId, orderInfoOffsets)
      }

    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      if (orderDetailOffsets == null || orderDetailOffsets.isEmpty) {
        MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroupId)
      } else {
        MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroupId, orderDetailOffsets)
      }

    var orderInfoOffsetRanges: Array[OffsetRange] = null
    val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDStream.transform(
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailKafkaDStream.transform(
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // *数据处理
    // 数据转换
    val orderInfoDStream: DStream[OrderInfo] = orderInfoOffsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        JSON.parseObject(value, classOf[OrderInfo])
      }
    )
    val orderDetailDStream: DStream[OrderDetail] = orderDetailOffsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        JSON.parseObject(value, classOf[OrderDetail])
      }
    )
    //    orderInfoDStream.print(100)
    //    orderDetailDStream.print(100)

    // 维度关联
    val orderInfoDimDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions(
      orderInfoIter => {
        val orderInfos: ListBuffer[OrderInfo] = ListBuffer[OrderInfo]()
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (orderInfo <- orderInfoIter) {
          // 用户
          val uid: Long = orderInfo.user_id
          val redisUserKey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUserKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)

          val gender: String = userInfoJsonObj.getString("gender")
          val birthday: String = userInfoJsonObj.getString("birthday")
          val age: Int = {
            val birthdayLd: LocalDate = LocalDate.parse(birthday)
            val nowLd: LocalDate = LocalDate.now()
            Period.between(birthdayLd, nowLd).getYears
          }

          orderInfo.user_gender = gender
          orderInfo.user_age = age
          // 地区
          val provinceID: Long = orderInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)

          val provinceName: String = provinceJsonObj.getString("name")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")

          orderInfo.province_name = provinceName
          orderInfo.province_area_code = provinceAreaCode
          orderInfo.province_3166_2_code = province3166
          orderInfo.province_iso_code = provinceIsoCode
          // 日期
          val createTime: String = orderInfo.create_time // Ex: 2025-04-29 10:12:02
          val createTimeArr: Array[String] = createTime.split(" ")

          val createDate: String = createTimeArr(0)
          val createHour: String = createTimeArr(1).split(":")(0)

          orderInfo.create_date = createDate
          orderInfo.create_hour = createHour

          orderInfos.append(orderInfo)
        }
        jedis.close()
        orderInfos.iterator
      }
    )
    //    orderInfoDimDStream.print(100)

    // 双流Join
    // 内连接 join  结果集取交集
    // 外连接
    //   左外连  leftOuterJoin   左表所有+右表的匹配, 分析清楚主(驱动)表 从(匹配表)表
    //   右外连  rightOuterJoin  左表的匹配 + 右表的所有,分析清楚主(驱动)表 从(匹配表)表
    //   全外连  fullOuterJoin   两张表的所有

    // 从数据库层面： order_info 表中的数据 和 order_detail表中的数据一定能关联成功.
    // 从流处理层面:  order_info 和  order_detail是两个流， 流的join只能是同一个批次的数据才能进行join
    //               如果两个表的数据进入到不同批次中， 就会join不成功.
    // 数据延迟导致的数据没有进入到同一个批次，在实时处理中是正常现象. 我们可以接收因为延迟导致最终的结果延迟.
    // 我们不能接收因为延迟导致的数据丢失.

    val orderInfoKVDStream: DStream[(Long, OrderInfo)] = orderInfoDimDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailKVDStream: DStream[(Long, OrderDetail)] = orderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))

    //    val orderJoinDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoKVDStream.join(orderDetailKVDStream)
    //    orderJoinDStream.print(1000)

    // 解决:
    //  1. 扩大采集周期, 治标不治本
    //  2. 使用窗口, 治标不治本, 还要考虑数据去重、Spark 状态的缺点
    //  3. 首先使用fullOuterJoin,保证join成功或者没有成功的数据都出现到结果中.
    //     让双方都多两步操作, 到缓存中找对的人， 把自己写到缓存中
    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoKVDStream.fullOuterJoin(orderDetailKVDStream)

    val orderWideDStream: DStream[OrderWide] = orderJoinDStream.mapPartitions(
      orderJoinIter => {
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        val orderWides: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        for ((_, (orderInfoOp, orderDetailOp)) <- orderJoinIter) {
          //. orderInfo有，orderDetail有
          if (orderInfoOp.isDefined) {
            //取出orderInfo
            val orderInfo: OrderInfo = orderInfoOp.get
            if (orderDetailOp.isDefined) {
              //取出orderDetail
              val orderDetail: OrderDetail = orderDetailOp.get
              //组装成orderWide
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
              //放入到结果集中
              orderWides.append(orderWide)
            }
            //. orderInfo有，orderDetail没有

            // orderInfo写缓存
            // 类型: string
            // key: ORDERJOIN:ORDER_INFO:ID
            // value: json
            // 写入API: set
            // 读取API: get
            // 是否过期: 24小时
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderInfo.id}"
            // set and expire
            jedis.setex(redisOrderInfoKey, 24 * 3600, JSON.toJSONString(orderInfo, JSONWriter.Feature.FieldBased))

            // orderInfo读缓存
            val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderInfo.id}"
            val orderDetails: util.Set[String] = jedis.smembers(redisOrderDetailKey)
            if (orderDetails != null && orderDetails.size() > 0) {
              import scala.collection.JavaConverters._
              for (orderDetailJson <- orderDetails.asScala) {
                val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                //组装成orderWide
                val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
                //加入到结果集中
                orderWides.append(orderWide)
              }
            }
          } else {
            //. orderInfo没有，orderDetail有
            val orderDetail: OrderDetail = orderDetailOp.get
            //读缓存
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderDetail.order_id}"
            val orderInfoJson: String = jedis.get(redisOrderInfoKey)
            if (orderInfoJson != null && orderInfoJson.nonEmpty) {
              val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              //组装成orderWide
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
              //加入到结果集中
              orderWides.append(orderWide)
            } else {
              //写缓存
              // 类型:    set
              // key:    ORDERJOIN:ORDER_DETAIL:ORDER_ID
              // value:  json, json ....
              // 写入API: sadd
              // 读取API: smembers
              // 是否过期: 24小时
              val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderDetail.order_id}"
              jedis.sadd(redisOrderDetailKey, JSON.toJSONString(orderDetail, JSONWriter.Feature.FieldBased))
              jedis.expire(redisOrderDetailKey, 24 * 3600)
            }
          }
        }
        jedis.close()
        orderWides.iterator
      }
    )

//    orderWideDStream.print(1000)

    // 写入ES
    orderWideDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          orderWideIter => {
            val orderWides: List[(String, OrderWide)] = orderWideIter.map(orderWide => (orderWide.detail_id.toString, orderWide)).toList
            if (orderWides.nonEmpty) {
              val head: (String, OrderWide) = orderWides.head
              val date: String = head._2.create_date
              val indexName: String = s"gmall_order_wide_$date"
              MyEsUtils.bulkSave(indexName, orderWides)
            }
          }
        )
        // 提交offset
        MyOffsetsUtils.saveOffset(orderInfoTopicName, orderInfoGroupId, orderInfoOffsetRanges)
        MyOffsetsUtils.saveOffset(orderDetailTopicName, orderDetailGroupId, orderDetailOffsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
