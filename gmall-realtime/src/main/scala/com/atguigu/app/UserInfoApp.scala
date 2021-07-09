package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.消费kafka中userInfo的数据
    val userInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER,ssc)


    //4.将json数据转为样例类，目的是为了获取到userId，然后将这个json写入redis
    userInfoKafkaDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val jedis: Jedis = new Jedis("hadoop102",6379)
        partition.foreach(record=>{
          val userInfo: UserInfo = JSON.parseObject(record.value(),classOf[UserInfo])
          //redisKey
          val redisKey: String = "userInfo:"+userInfo.id
          jedis.set(redisKey,record.value())
        })
        jedis.close()
      })
    })

//    //4.将json数据转为样例类
//    val userInfoDStream = userInfoKafkaDStream.mapPartitions(partition => {
//      partition.map(record => {
//        val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
//        userInfo
//      })
//    })

    //5.将数据写入Redis
//    userInfoDStream.foreachRDD(rdd=>{
//      rdd.foreachPartition(partition=>{
//        implicit val formats = org.json4s.DefaultFormats
//        //创建redis连接
//        val jedis: Jedis = new Jedis("hadoop102",6379)
//        partition.foreach(userInfo=>{
//          val userInfoStr: String = Serialization.write(userInfo)
//          //redisKey
//          val redisKey: String = "userInfo:"+userInfo.id
//          jedis.sadd(redisKey,userInfoStr)
//        })
//        jedis.close()
//      })
//    })

    userInfoKafkaDStream.map(_.value()).print()


    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
