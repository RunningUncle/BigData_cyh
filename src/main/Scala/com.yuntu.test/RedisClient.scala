package com.yuntu.test

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

object RedisClient {
  val redisHost = ""
  val redisPort = 6
  val redisTimeout = 30
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)

  //  def main(args: Array[String]): Unit = {
  //    val dbIndex = 0
  //
  //    val jedis = RedisClient.pool.getResource
  //    jedis.select(dbIndex)
  //    jedis.set("test", "1")
  //    println(jedis.get("test"))
  //    RedisClient.pool.returnResource(jedis)
  //
  //  }


  /**
    * 获取redis客户端 记得用完需要关闭
    *
    * @param host
    * @param port
    * @return
    */
  def getRedisClient(host: String, port: Int): Jedis = {
    pool.getResource
  }

  /**
    * 关闭redis客户端
    *
    * @param redisClient
    */
  def closeRedisClient(redisClient: Jedis): Unit = {
    redisClient.close()
  }

  /**
    * 存 数据
    *
    * @param redisClient
    * @param key
    * @param value
    */
  def put(redisClient: Jedis, key: String, value: String): Unit = {
    redisClient.set(key, value)
  }

  /**
    * 获取值
    *
    * @param redisClient
    * @param key
    * @return
    */
  def get(redisClient: Jedis, key: String): String = {
    redisClient.get(key)
  }
}
