package com.example.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable
import java.util

/**
 * Kafka工具类
 * KafkaVersion: 3.9.0
 */
object MyKafkaUtils {
  /**
   * Kafka消费者配置
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(Const.KAFKA_BOOTSTRAP_SERVERS),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    // enable.auto.commit       Default: true
    // auto.commit.interval.ms  Default: 5000(5 seconds)
    // auto.offset.reset        Default: latest
  )

  /**
   * 获取 KafkaDStream，使用默认的offset策略
   *
   * @param ssc     StreamingContext
   * @param topic   主题
   * @param groupId 消费组ID
   * @return Kafka DStream
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs)
    )
    kafkaDStream
  }

  /**
   * 获取 KafkaDStream，使用指定的offset策略
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offsets: Map[TopicPartition, Long]) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs, offsets)
    )
    kafkaDStream
  }

  private val producer: KafkaProducer[String, String] = createProducer()

  private def createProducer(): KafkaProducer[String, String] = {
    val producerConfigs: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(Const.KAFKA_BOOTSTRAP_SERVERS))
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // acks       Default: all
    // batch.size Default: 16384(16kb)
    // linger.ms  Default: 5
    // retries    Default: 2147483647(max int)
    // enable.idempotence(幂等性) Default: true
    val producer = new KafkaProducer[String, String](producerConfigs)
    producer
  }

  /**
   * 发送消息到Kafka, 使用默认的粘性分区策略
   *
   * @param topic 主题
   * @param msg   消息内容
   */
  def send(topic: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
   * 发送消息到Kafka, 使用默认的粘性分区策略
   *
   * @param topic 主题
   * @param key   键
   * @param msg   消息内容
   */
  def send(topic: String, key: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  def close(): Unit = {
    Option(producer).foreach(_.close())
  }

  /**
   * 刷写, 将缓存中的数据刷写到磁盘
   */
  def flush(): Unit = {
    producer.flush()
  }
}
