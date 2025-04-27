package com.example.utils

import org.junit.jupiter.api.Test
import redis.clients.jedis.Jedis

class MyRedisUtilsTest {
    @Test
    def getJedisFromPool(): Unit = {
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        println(jedis.ping())
        jedis.close()
    }

    @Test
    def setKeyAndValue(): Unit = {
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        jedis.set("fuck", "you")
        println(jedis.get("fuck"))
        jedis.close()
    }
}