package com.example.app

import com.alibaba.fastjson2.{JSON, JSONObject}
import com.example.utils.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.util

/**
 * 业务数据消费分流
 *
 * 1. 准备实时环境
 * 2. 从redis中读取偏移量
 * 3. 从kafka中消费数据
 * 4. 提取偏移量结束点
 * 5. 数据处理
 * 5.1 转换数据结构
 * 5.2 分流
 * 事实数据 => Kafka
 * 维度数据 => Redis
 * 6. flush Kafka的缓冲区
 * 7. 提交offset
 */
object OdsBaseDbApp {

  def main(args: Array[String]): Unit = {

    // 1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topicName: String = "ODS_BASE_DB"
    val groupId: String = "ODS_BASE_DB_GROUP"

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
    // 5.1 转换数据结构
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        val dataJson: String = consumerRecord.value()
        val jSONObject: JSONObject = JSON.parseObject(dataJson)
        jSONObject
      }
    )
    //    jsonObjDStream.print(100)

    // 5.2 分流

    // Redis连接写到哪里???
    //  foreachRDD外面: driver，连接对象不能序列化，不能传输
    //  foreachRDD里面, foreachPartition外面 : driver，连接对象不能序列化，不能传输
    //  foreachPartition里面, 循环外面: executor，每分区数据开启一个连接，用完关闭
    //  foreachPartition里面, 循环里面: executor，每条数据开启一个连接，用完关闭，太频繁
    //
    jsonObjDStream.foreachRDD(
      rdd => {
        val redisFactKeys : String = "FACT:TABLES"
        val redisDimKeys : String = "DIM:TABLES"
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        // 事实表清单、维度表清单
        val factTables: util.Set[String] = jedis.smembers(redisFactKeys)
        val dimTables: util.Set[String] = jedis.smembers(redisDimKeys)
        println("factTables: " + factTables)
        println("dimTables: " + dimTables)
        val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
        val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
        jedis.close()

        rdd.foreachPartition(
          jsonObjIter => {
            val jedis: Jedis = MyRedisUtils.getJedisFromPool()
            for (jsonObj <- jsonObjIter) {
              val operType: String = jsonObj.getString("type")

              val opValue: String = operType match {
                case "bootstrap-insert" => "I"
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }
              if (opValue != null) {
                val tableName: String = jsonObj.getString("table")
                val data: String = jsonObj.getString("data")

                if (factTablesBC.value.contains(tableName)) {
                  val dwdTopicName: String = s"DWD_${tableName.toUpperCase}_${opValue}"
                  MyKafkaUtils.send(dwdTopicName, data)
                }
                if (dimTablesBC.value.contains(tableName)) {
                  val dataObj: JSONObject = jsonObj.getJSONObject("data")
                  val id: String = dataObj.getString("id")
                  val redisKey : String = s"DIM:${tableName.toUpperCase}:$id"
                  jedis.set(redisKey, dataObj.toJSONString())
                }
              }
            }
            jedis.close()
            MyKafkaUtils.flush()
          }
        )
        //提交offset
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}