package com.asiainfo.gmall.realtime.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.asiainfo.gmall.common.constants.GmallConstants
import com.asiainfo.gmall.realtime.bean.StartUpLog
import com.asiainfo.gmall.realtime.utils.{MyKafkaUtil, RedisUtil}
import kafka.utils.Json
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/*
	消费kafka中的数据。
	利用redis过滤当日已经计入的日活设备。
	把每批次新增的当日日活信息保存到HBASE或ES中。
	从ES中查询出数据，发布成数据接口，通可视化化工程调用。

 */
object MyApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dauapp")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val sc: SparkContext = ssc.sparkContext
    import org.apache.phoenix.spark._
    val startUpStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)
    //要先进行查看，看看能否从kafka中消费到实时数据
//    startUpStream.foreachRDD(rdd=>{
//      println(rdd.map(_.value()).collect().mkString(","))
//    })
    // 先map转换，得到一个string的rdd,再进行遍历
//    startUpStream.map(_.value()).foreachRDD(rdd=>{
//      println(rdd.collect().mkString(","))
//    })
    //把数据封装到样例类中，map转换

    //TODO 2 数据流 转换 结构变成case class 补充两个时间字段

//    val startUpLog: DStream[StartUpLog] = startUpStream.map(cr => {
    ////      cr.value()
    ////    }).map(JSON.parseObject(_, classOf[StartUpLog]))
//    startUpLog
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDS: DStream[StartUpLog] = startUpStream.map {
      record => {
        val jsonstr: String = record.value()
        val startUp: StartUpLog = JSON.parseObject(jsonstr, classOf[StartUpLog])
        val ts: Long = startUp.ts
        val datestr: String = dateFormat.format(ts)
        val hourDate: Array[String] = datestr.split(' ')
        startUp.logDate = hourDate(0)
        startUp.logHour = hourDate(1)
        startUp
      }
    }
    //如果我们处理逻辑跟不上生产速度，先缓存起来
    startUpLogDS.cache()
    //TODO 3 利用用户清单进行过滤 去重  只保留清单中不存在的用户访问记录,利用redis中的set
    // 单独靠redis里面的值取出来去重，过滤得不彻底，原因是同一批次的数据也可能存在重复的，
    // 但并没有去重，我们还需要同一批次做一个去重


    val filterDstream: DStream[StartUpLog] = startUpLogDS.transform(
      rdd => {
        //以下代码在driver端执行，每个周期执行一次，更新一下广播变量
        println("去重前="+rdd.count())
        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        val date: String = formatter.format(new Date())
        var key = "dau:" + date
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //查找当天key对应的所有值
        val values: util.Set[String] = jedisClient.smembers(key)
        //TODO 用完连接必须要关闭，否则连接池会奔溃
        jedisClient.close()
        //把这批数据用一个广播变量存起来，就不用每个excutor去传输一次redis数据
        val dauMidBC: Broadcast[util.Set[String]] = sc.broadcast(values)
        //executor 执行
        val filterRDD: RDD[StartUpLog] = rdd.filter(
          startUpLog => {
            val flag: Boolean = dauMidBC.value.contains(startUpLog.mid)
            !flag
          })
        println("第一次去重后="+filterRDD.count())
        filterRDD
      })
    //TODO 3.1 同一批次内部数据去重，相同的mid只保留第一条数据
    val groupByKeyDstream: DStream[(String, Iterable[StartUpLog])] = filterDstream.map(startUpLog=>(startUpLog.mid,startUpLog)).groupByKey()
    val finalFilterDstream: DStream[StartUpLog] = groupByKeyDstream.flatMap {
      case (key, iter) => {
        val topList: List[StartUpLog] = iter.toList.sortWith((start1, start2) => start1.ts < start2.ts).take(1)
        topList
      }
    }
//    println("彻底去重后："+finalFilterDstream)

    //这样每次都会去redis里面判断这个mid是否存在，太消耗时间和网络
//    val filterDstream: DStream[StartUpLog] = filterDstream
//    filterDstream
//    算子内部的代码在executor端执行，去重方法一，存在优化空间
//    val filterDs: DStream[StartUpLog] = startUpLogDS.filter(
//      startUpLog => {
//      val jedisClient: Jedis = RedisUtil.getJedisClient
//      var key = "dau:" + startUpLog.logDate
//      val flag: Boolean = jedisClient.sismember(key, startUpLog.mid)
//      !flag
//    })
//    filterDs


    //TODO 4 把用户访问清单保存到redis中
    // type set key [dau:2020-08-07] value [mid]
    finalFilterDstream.foreachRDD(rdd=>{
      println("彻底去重后="+rdd.count())
      //这一步可能会导致数据变慢，会阻塞kafka队列
      rdd.saveToPhoenix(GmallConstants.ES_INDEX_DAU,Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration,Some(GmallConstants.ZKURL))
      rdd.foreachPartition(iter=>{
//        println("获取一个redis conn")
        val jedis: Jedis = RedisUtil.getJedisClient //executor
        for (elem <- iter) {

          var key="dau:"+elem.logDate
          jedis.sadd(key,elem.mid)

        }
//        println("调用关闭连接的方法")
        jedis.close()
      })
    })


//    startUpLogDS.foreachRDD(rdd=>{
//      //把数据写到redis 中缓存起来,每一条记录都会连接一次redis
//      rdd.foreach(startUpLog=>{
//
//        val jedis = new Jedis("s102",6379)
//        val date: String = startUpLog.logDate
//        var key="dau:"+date
//        jedis.sadd(key,startUpLog.mid)
//        //关闭redis
//        jedis.close()
//      })
//
//    })

    //让采集器一直运行
    ssc.start()
    ssc.awaitTermination()
  }

}

