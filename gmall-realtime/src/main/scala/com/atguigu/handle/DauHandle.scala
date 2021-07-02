package com.atguigu.handle

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandle {
  /**
    * 进行批次内去重
    * @param fileByRedisDStream
    */
  def filterByGroup(fileByRedisDStream: DStream[StartUpLog]) = {

      //a.将数据转为kv格式，k（mid，logdate） v（log）
      val midAndLogDateToLogDStream: DStream[((String, String), StartUpLog)] = fileByRedisDStream.map(startUpLog => {
        ((startUpLog.mid, startUpLog.logDate), startUpLog)
      })

      //b.将相同key的数据聚和到一块
      val midAndLogDateToIterLogDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndLogDateToLogDStream.groupByKey()

      //c.对数据按照时间戳进行排序
      val midAndLogDateToListLog: DStream[((String, String), List[StartUpLog])] = midAndLogDateToIterLogDStream.mapValues(iter => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      })

      //d.将list集合中的数据打散出来
      val listDStream = midAndLogDateToListLog.flatMap(_._2)

      listDStream
  }

  /**
    * 利用Redis做批次间去重
    *
    * @param startUpLogDStram
    */
  def filterByRedis(startUpLogDStram: DStream[StartUpLog],sc:SparkContext) = {
   /* val value: DStream[StartUpLog] = startUpLogDStram.filter(startUpLog => {
      //a.创建redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //b.查询redis中保存的Mid
      //设置Rediskey
      val redisKey: String = "DAU:" + startUpLog.logDate
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //c.拿当前的mid与redis中已经保存的mid做对比，如果有的话扔掉，没有的话保留
      val bool: Boolean = mids.contains(startUpLog.mid)

      jedis.close()
      !bool
    })
    value
*/
    //方案二：在每个分区内获取一次连接
  /*  val value: DStream[StartUpLog] = startUpLogDStram.mapPartitions(partition => {
      //在分区下创建连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val logs: Iterator[StartUpLog] = partition.filter(startUpLog => {
        //b.查询redis中保存的Mid
        //设置Rediskey
        val redisKey: String = "DAU:" + startUpLog.logDate
        val mids: util.Set[String] = jedis.smembers(redisKey)

        //c.拿当前的mid与redis中已经保存的mid做对比，如果有的话扔掉，没有的话保留
        val bool: Boolean = mids.contains(startUpLog.mid)

        !bool
      })

      //关闭连接
      jedis.close()
      logs
    })
    value*/

    //方案三：每个批次下获取一次连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStram.transform(rdd => {
      //a.获取redis连接(dirver)
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //b.获取redis中的数据
      //设置Rediskey
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //c.将dirver端的数据广播到executor端
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //d.过滤，如果当前的数据在广播出来的数据（redis中保存的去重后的数据）中存在的话就过滤掉
      val filterMid: RDD[StartUpLog] = rdd.filter(startupLog => {
        !midBC.value.contains(startupLog.mid)
      })
      jedis.close()
      filterMid
    })
    value

  }

  /**
    * 将去重后的mid保存至Redis
    *
    * @param startUpLogDStram
    */
  def saveMidToRedis(startUpLogDStram: DStream[StartUpLog]) = {
    startUpLogDStram.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //a.创建redis连接
        val jedis: Jedis = new Jedis("hadoop102",6379)

        //b.将数据保存至Redis
        partition.foreach(startUpLog=>{
          //设置Rediskey
          val redisKey: String = "DAU:"+startUpLog.logDate
          jedis.sadd(redisKey,startUpLog.mid)
        })
        //关闭连接
        jedis.close()
      })
    })

  }

}
