package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.消费kafka数据
    //订单主题
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    //订单明细主题
    val detailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //4.将从kafka读取过来的数据转为样例类,并且是Kv类型k—>orderId,因为下面需要进行join，要指定连接条件，默认就是这个key
    val orderInfoDStream = orderInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        //a.将数据转为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //b.补全create_date字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)

        //c.补全create_hour字段
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)

        (orderInfo.id, orderInfo)

      })
    })
    val orderDetailDStream = detailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })

    //5.将两张表的数据Join到一块
    //    val joinDStream: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(orderDetailDStream)
    val fulljoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.通过加缓存的方式解决数据丢失问题
    val noUserDStream: DStream[SaleDetail] = fulljoinDStream.mapPartitions(partition => {
      implicit val formats = org.json4s.DefaultFormats
      //TODO 创建list集合用来存放SaleDetail样例类

      //创建Redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      partition.foreach { case (orderid, (infoOpt, detailOpt)) =>

        //创建redisKey
        val infoRedisKey: String = "orderInfo:" + orderid
        val detailRedisKey: String = "orderDetail:" + orderid

        //1.判断orderInfo数据是否存在
        if (infoOpt.isDefined) {
          //orderInfo存在
          val orderInfo: OrderInfo = infoOpt.get
          //2.判断orderDetail数据是否存在
          if (detailOpt.isDefined) {
            //orderDetail存在
            val orderDetail: OrderDetail = detailOpt.get
            val saleDetail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
            details.add(saleDetail)
          }

          //JSON.toJSONString(orderInfo) 样例类不能这样直接转为JSON
          //3.将样例类转为JSON
          val orderInfoJson: String = Serialization.write(orderInfo)
          //4.将orderInfo数据写入Redis，并设置过期时间
          jedis.set(infoRedisKey, orderInfoJson)
          jedis.expire(infoRedisKey, 30)

          //5.查询对方缓存（orderDetail）
          if (jedis.exists(detailRedisKey)) {
            val orderDetails: util.Set[String] = jedis.smembers(detailRedisKey)
            for (elem <- orderDetails.asScala) {
              //6.将查询出来的JSON字符串转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            }
          }
        } else {
          //orderInfo不存在
          if (detailOpt.isDefined) {
            //6.orderDetail存在，所以获取orderDetail数据
            val orderDetail: OrderDetail = detailOpt.get
            if (jedis.exists(infoRedisKey)) {
              //7.缓存中存在能join上的orderInfo数据
              val infoStr: String = jedis.get(infoRedisKey)
              //8.因为要写入SaleDetail样例类，需要传入的是样例类，所以将从redis读过来的json字符串转为样例类
              val orderInfo: OrderInfo = JSON.parseObject(infoStr, classOf[OrderInfo])
              //9.将数据关联到SaleDetail
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(detail)
            } else {
              //10.如果对方缓存中还没有能关联上的数据，那有可能是来早了，需要把自己写入缓存
              val orderDetailJson: String = Serialization.write(orderDetail)
              jedis.sadd(detailRedisKey, orderDetailJson)
              jedis.expire(detailRedisKey, 30)
            }
          }
        }
      }
      //关闭redis连接
      jedis.close()
      details.asScala.toIterator
    })

    //7.关联userInfo数据
    val saleDetailDStream: DStream[SaleDetail] = noUserDStream.mapPartitions(partition => {
      //创建Redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        //a.根据redisKey查询UserInfo
        val userRediskey: String = "userInfo:" + saleDetail.user_id
        val userInfoStr: String = jedis.get(userRediskey)

        //b.查询出来的字符串类型的userInfo数据转为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      jedis.close()
      details
    })
    saleDetailDStream.print()

    //8.将SaleDetail数据写入ES
    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })

        MyEsUtil.insertBulk(GmallConstants.ES_SALE_DETAIL+"0225",list)
      })
    })

    //
    //    orderInfoKafkaDStream.map(_.value()).print()
    //
    //    detailKafkaDStream.map(_.value()).print()

    //开启任务
    ssc.start()
    ssc.awaitTermination()


  }

}
