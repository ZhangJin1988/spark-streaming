package cn.sheep.tools

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * author: sheep.Old 
  * qq: 64341393
  * Created 2018/6/14
  */
object Jpools {

    private lazy val poolConfig = new GenericObjectPoolConfig()
    poolConfig.setMaxTotal(1000) // 支持最大的连接数
    poolConfig.setMaxIdle(5)// 支持最大的空闲连接
    // ...
    private lazy val jedisPool = new JedisPool(poolConfig, "10.172.50.12")

    def getJedis = {
        val jedis = jedisPool.getResource // 从池子中返回一个连接
        jedis.select(0) // 8号
        jedis
    }



}
