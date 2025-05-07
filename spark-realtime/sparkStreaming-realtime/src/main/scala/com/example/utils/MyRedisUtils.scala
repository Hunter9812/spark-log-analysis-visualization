package com.example.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Redis 工具类，用于获取 Jedis 连接，操作 Redis
 */
object MyRedisUtils {
  // 使用 lazy val 实现线程安全的懒加载
  private lazy val jedisPool: JedisPool = {
    val config = new JedisPoolConfig()
    config.setMaxTotal(100)
    config.setMaxIdle(20)
    config.setMinIdle(20)
    config.setBlockWhenExhausted(true)
    config.setMaxWaitMillis(5000)
    config.setTestOnBorrow(true)

    val host: String = MyPropsUtils(Const.REDIS_HOST)
    val port: String = MyPropsUtils(Const.REDIS_PORT)

    new JedisPool(config, host, port.toInt)
  }

  def getJedisFromPool(): Jedis = jedisPool.getResource

  def close(): Unit = {
    if (!jedisPool.isClosed) jedisPool.close()
  }

}
