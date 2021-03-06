package com.yt.test

import com.google.gson.{JsonObject, JsonParser}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_Redis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(5))
    ssc.checkpoint("")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer], //StringDecoder
      "group.id" -> "",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("boxoffice")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    stream.map(item => {
      item.value()
      //      print(value)
      //      value
    }).map(item => {
      caculatePf(item)
    }).updateStateByKey(func,
      new HashPartitioner(ssc.sparkContext.defaultParallelism),
      rememberPartitioner = true)
      .foreachRDD(rdd => {
        rdd.foreach(item => {
          saveToRedis(item)
        })
      })

    ssc.start
    ssc.awaitTermination()

    RedisClient.closeRedisClient(RedisClient.getRedisClient("172.21.32.25", 6379))
  }

  val func: Iterator[(String, Seq[Double], Option[Double])] => Iterator[(String, Double)] =
    (it: scala.Iterator[(String, scala.Seq[Double], scala.Option[Double])]) => {
      val tuples: Iterator[(String, Double)] = it.map(item => {
        //println(item._2.toBuffer)
        val d: Double = item._3.getOrElse(0.0)
        //println("======"+d)
        var sum: Double = item._2.sum
        sum += d
        //println("--------------------"+sum)
        (item._1, sum)
      })
      tuples
    }

  /**
    * 解析json数据 获取sales 字段
    * {
    * "cinema_code": "44131271",
    * "onlineSales": 980.0,
    * "reportTime": "2018-11-27 06:04:45",
    * "service": 120.0,
    * "onlineSalesCount": 49,
    * "saleCount": 49,
    * "sales": 1100.0,
    * "screenCode": "0000000000000003",
    * "sessionDatetime": "2018-11-26T20:30:00",
    * "businessDate": "2018-11-26",
    * "seats": null,
    * "sessionCode": "0618112500312054",
    * "filmCode": "074102212018"
    * }
    *
    * @param jsonStr
    * @return
    */

  def caculatePf(jsonStr: String): (String, Double) = {
    val jsonParser = new JsonParser
    var value: Double = 0.0
    val jobj = jsonParser.parse(jsonStr).asInstanceOf[JsonObject]
    if (jobj.has("sales")) {
      value = jobj.get("sales").getAsDouble
    }

    var key = ""
    if (jobj.has("filmCode")) {
      key = jobj.get("filmCode").getAsString
    }
    (key, value)
  }


  def saveToRedis(res: (String, Double)): Unit = {
    RedisClient.getRedisClient(null, 0).set("YPDATA20181212:" + res._1, res._2.toString)
  }

}

