package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handle.DauHandle
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.获取kafka中StartUp（启动日志）主题的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //4.将json数据转为样例类（StartUpLog）并且，补全LogDate（精确到天）和LogHour(精确到小时）字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStram: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //a.将数据转为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        //b.补全Logdate字段
        val times: String = sdf.format(new Date(startUpLog.ts))
        startUpLog.logDate = times.split(" ")(0)

        //c.补全LogHour字段
        startUpLog.logHour = times.split(" ")(1)

        startUpLog
      })
    })
//    startUpLogDStram.print()

    //优化：对DStream缓存
    startUpLogDStram.cache()

    //5.进行批次间去重
    val fileByRedisDStream: DStream[StartUpLog] = DauHandle.filterByRedis(startUpLogDStram,ssc.sparkContext)

    //优化：对DStream缓存
    fileByRedisDStream.cache()

    //打印原始数据条数
    startUpLogDStram.count().print()
    //打印经过批次间去重后的数据条数
    fileByRedisDStream.count().print()

    //6.进行批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandle.filterByGroup(fileByRedisDStream)
    filterByGroupDStream.cache()
    //打印经过批次内去重后的数据条数
    filterByGroupDStream.count().print()

    //7.将去重后的mid保存至Redis
    DauHandle.saveMidToRedis(filterByGroupDStream)


    //8.将去重后的明细数据保存至Hbase
    filterByGroupDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix(
        "GMALL0225_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )

    })

//    //测试kafka数据
//    kafkaDStream.foreachRDD(rdd=>{
//      rdd.foreachPartition(partition=>{
//        partition.foreach(record=>{
//          println(record.value())
//        })
//      })
//    })



    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
