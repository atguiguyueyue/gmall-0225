package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.获取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)

    //4.将json数据转为样例类，并返回kv类型数据，为了方便下面进行groupByKey
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        //补全字段
        val times: String = sdf.format(new Date(eventLog.ts))
        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)

        (eventLog.mid, eventLog)
      })
    })

    //5.开窗
    val midToLogWindowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    //6.将相同mid的数据聚和到一块groupByKey
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = midToLogWindowDStream.groupByKey()

    //7.根据条件筛选数据，没有浏览商品->领优惠券->符合这些行为的用户数，是否大于等于三
    val boolToAlertInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>
        //创建Set集合用来存放用户id
        val uids: util.HashSet[String] = new util.HashSet[String]()
        //创建Set集合用来存放领优惠券涉及的商品id
        val itemIds: util.HashSet[String] = new util.HashSet[String]()
        //创建list集合用来存放事件
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //定义标志位，用来判断是否有浏览商品行为
        var bool: Boolean = true
        //遍历迭代器中的数据
        breakable {
          iter.foreach(log => {
            //将用户行为存放到集合中
            events.add(log.evid)
            if ("clickItem".equals(log.evid)) {
              bool = false
              //跳出循环
              break()
              //领优惠券行为
            } else if ("coupon".equals(log.evid)) {
              //将用户id存放set集合
              uids.add(log.uid)
              //将涉及的商品id放入集合
              itemIds.add(log.itemid)
            }
          })
        }
        //生成疑似预警日志
        (uids.size() >= 3 && bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    })

    //8.生成预警日志
    val couponAlertInfo: DStream[CouponAlertInfo] = boolToAlertInfoDStream.filter(_._1).map(_._2)

    couponAlertInfo.print()

    //9.将预警日志写入ES
    couponAlertInfo.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val list: List[(String, CouponAlertInfo)] = iter.toList.map(log => {
          (log.mid + log.ts / 1000 / 60, log)
        })
          MyEsUtil.insertBulk(GmallConstants.ES_ALERT,list)
      })
    })

    //10.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
