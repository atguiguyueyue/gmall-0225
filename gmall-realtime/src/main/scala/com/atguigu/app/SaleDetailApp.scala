package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis
import collection.JavaConverters._

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
    fulljoinDStream.mapPartitions(partition => {
      implicit val formats=org.json4s.DefaultFormats

      //创建Redis连接
      val jedis: Jedis = new Jedis("hadoop102",6379)
      partition.map { case (orderid, (infoOpt, detailOpt)) =>
        //TODO 创建list集合用来存放SaleDetail样例类
        val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

        //创建redisKey
        val infoRedisKey: String = "orderInfo:"+orderid
        val detailRedisKey: String = "orderDetail:"+orderid

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
          jedis.set(infoRedisKey,orderInfoJson)
          jedis.expire(infoRedisKey,10)

          //5.查询对方缓存（orderDetail）
          if (jedis.exists(detailRedisKey)){
            val orderDetails: util.Set[String] = jedis.smembers(detailRedisKey)
            for (elem <- orderDetails.asScala) {
              //6.将查询出来的JSON字符串转为样例类
              val orderDetail: OrderDetail = JSON.parseObject(elem,classOf[OrderDetail])
              val detail: SaleDetail = new SaleDetail(orderInfo,orderDetail)
              details.add(detail)
            }

          }

        }

      }
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
