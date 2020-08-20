package com.asiainfo.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.asiainfo.gmall.common.constants.GmallConstants
import com.asiainfo.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.asiainfo.gmall.realtime.utils.{MyEsUtil, MyKafkaUtil, MySelfEsUtil, RedisUtil}
import io.searchbox.client.JestClientFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer
import collection.JavaConversions._

object SaleInfoApp {
  /*
      todo 1 order_info 和order_detail关联，实时关联，微批次之间的关联可能不完整，会漏掉数据
       order_info 和order_detail 是一对多的关系，关联使用order_info 作为主表，order_detail作为从表
       关联上的情况： order_detail 不为None,否则为找到对应的order_detail
        order_info 为None ??，使用full outer join 全关联，结果都会出现
   */
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("sale_info").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    //消费kafka数据
    val orderInfoDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_INFO, ssc)
    val orderDetailDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)
    //todo 2 转换成 k-v类型的结构，使用样例类
    // oder_info 需要把时间字段拆分一下，联系人电话要进行脱敏处理
    //    val formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val orderInfoPairDstream: DStream[(String, OrderInfo)] = orderInfoDstream.map(
      record => {
        val orderinfoJson: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(orderinfoJson, classOf[OrderInfo])
        //        2020-08-13 00:02:50 业务数据库时间字段是这个类型
        val create_time: String = orderInfo.create_time
        //        formater.format(create_time)
        //        val dateStr: String = formater.format(create_time)
        val arrays: Array[String] = create_time.split(' ')
        if (arrays.size == 2) {
          orderInfo.create_date = arrays(0)
          orderInfo.create_hour = arrays(1).split(':')(0)
        }
        val consignee_tel: String = orderInfo.consignee_tel
        val before3: (String, String) = consignee_tel.splitAt(3)
        val after4: (String, String) = consignee_tel.splitAt(7)
        orderInfo.consignee_tel = before3._1 + "****" + after4._2
        (orderInfo.id, orderInfo)
      }

    )

    val orderDetailPairDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map(
      record => {
        val orderDetailStr: String = record.value()
        val detailBean: OrderDetail = JSON.parseObject(orderDetailStr, classOf[OrderDetail])
        (detailBean.order_id, detailBean)
      }
    )
    //    orderDetailPairDstream
    //    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoPairDstream.join(orderDetailPairDstream)
    // todo 3 两个（k,v）结构的流进行fullouterJoin 操作
    val fullOuterJoinResultDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoPairDstream.fullOuterJoin(orderDetailPairDstream)
    //todo 4 关联两张表，匹配上的存在一个list集合中


    //    implicit val formater = org.json4s.DefaultFormats
    //mapPartitions,一次处理一个分区，一个分区的数据的全部处理完，该分区的引用才会被释放，消耗内存
    //  参数是一个iter，要求返回也得是一个iter
    val saleDetailDstream: DStream[SaleDetail] = streamJoinStream(fullOuterJoinResultDstream)

    //todo 4.6 和user_info 维表关联，把宽表信息补充完整


    //todo 4.7 从缓存中获取userinfo信息，把结构转化为可以写入redis的list形式[(key,saleDetailJson)]
    val listBuffer = new ListBuffer[SaleDetail]
    val saleDetailWideTable: DStream[SaleDetail] = saleDetailDstream.mapPartitions(
      iter => {
        //以下代码在mapPartitions算子内部，代码在executor端执行
        val jedis: Jedis = RedisUtil.getJedisClient
        for (saleDetail <- iter) {
          var user_infoKey = GmallConstants.FULLOUTERJOIN_REDIS_USERINFOKEY_PRE + saleDetail.user_id
          val user_infoJson: String = jedis.get(user_infoKey)
          val userInfo: UserInfo = JSON.parseObject(user_infoJson, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo)
          listBuffer += saleDetail

        }
        jedis.close()
        listBuffer.toIterator

      }
    )
    //    saleDetailWideTable 保存到es
//    saleDetailWideTable.
        saleDetailWideTable.foreachRDD(
          rdd=> {
            rdd.foreachPartition(
              saleDetailIter => {
                val saleBulkIter: Iterator[(String, SaleDetail)] = saleDetailIter.map(sale_detail => (sale_detail.order_detail_id, sale_detail))
//                MySelfEsUtil.insertBulk(GmallConstants.ES_INDEX_SALE_DETAIL,saleBulkIter.toList)
                  MyEsUtil.saveToEsBatch(saleBulkIter.toList, GmallConstants.ES_INDEX_SALE_DETAIL, "_doc")
              }
            )
          }
        )

    //自己的方法
//    saleDetailWideTable.foreachRDD(
//      rdd => {
//
//        val newRDD: RDD[(String, SaleDetail)] = rdd.map(saleDetail => (saleDetail.order_detail_id, saleDetail))
//        newRDD.foreachPartition(iter => {
//          MyEsUtil.saveToEsBatch(iter.toList, GmallConstants.ES_INDEX_SALE_DETAIL, "_doc")
//        })
//      }
//    )

//todo 4.8同步 user_info表信息到redis中，放在写入es之后，我个人觉得会导致部分数据没有及时关联上
    val userInfoDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER_INFO, ssc)
    //    println("获取user_info实时流")
    val userInfoBeanDstream: DStream[UserInfo] = userInfoDstream.map(
      record => {
        val userinfoJson: String = record.value()
        JSON.parseObject(userinfoJson, classOf[UserInfo])

      }

    )
    userInfoBeanDstream.foreachRDD(
      rdd => {
        //这里写代码是在driver执行
        rdd.foreachPartition(
          iter => {
            //这里的 代码是在executor端执行，一个分区执行一次
            implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
            val jedisClient: Jedis = RedisUtil.getJedisClient
            for (user_info <- iter) {

              val user_infoKey = GmallConstants.FULLOUTERJOIN_REDIS_USERINFOKEY_PRE + user_info.id
              //存放到redis 中 type  string key value
              jedisClient.set(user_infoKey, Serialization.write(user_info))
            }
            jedisClient.close()
          }
        )

      }
    )




    //todo 4.8 执行行动算子，看结果
    //    saleDetailDstream.foreachRDD(
    //      rdd=> println(rdd.collect().mkString("\n"))
    //    )

    //TODO 5 map 一次处理一条数据，相对来说，效率更低一些
    //    fullOuterJoinResultDstream.map {
    //      //以下代码在executor端执行
    //      case (orderId, (orderInfoOpt, orderDetailOpt)) =>
    //        if (orderInfoOpt != None) {
    //          val orderInfo: OrderInfo = orderInfoOpt.get
    //          if (orderDetailOpt != None) {
    //            val orderDetail: OrderDetail = orderDetailOpt.get
    //            val saleDetail = new SaleDetail(orderInfo, orderDetail)
    //            listBuffer += saleDetail
    //          }
    //          //todo 把orderinfo信息保存到redis 缓存中
    //          //todo 关键点scala 样例类转成json字符串，不能用我们java里面的json工具，scala识别不了，需要scala专属的json工具才行
    //          //      jedisClient.set("orderInfo:"+orderId,JSON.toJSONString(orderInfo))
    //          var orderInfoKey = "orderInfo:" + orderId
    //          val OrderJsonString: String = Serialization.write(orderInfo)
    //          //todo redis连接会频繁获取，释放，不太好
    //          val jedisClient: Jedis = RedisUtil.getJedisClient
    //          jedisClient.set(orderInfoKey, OrderJsonString)
    //          jedisClient.close()
    //          //todo 4.3 主表去查缓存
    //
    //        }
    //
    //    }


    //    import collection.JavaConversions._
    //    fullOuterJoinResultDstream.foreachRDD(
    //      rdd=>
    //        println(rdd.collect().mkString("\n"))
    //
    //        )
    //两种打印方法
    //    fullOuterJoinResultDstream.foreachRDD(
    //      rdd=>
    //        rdd.foreach{
    //          case(orderId,(orderInfoOpt,orderDetailOpt))=>
    //            println(orderId+"||"+orderInfoOpt.get+"||"+orderDetailOpt.get)
    //        }
    //    )


    //        println(rdd.collect().mkString("\n"))
    //        rdd.map{
    //          case (orderId,(orderInfoOpt,orderDetailOpt))=>
    //          if (orderInfoOpt!=None) {
    //            val orderInfoBean: OrderInfo = orderInfoOpt.get
    //            if (orderDetailOpt != None) {
    //              //完全匹配上的
    //              val saleDetailBean = new SaleDetail(orderInfoBean, orderDetailOpt.get)
    //              //两种往可变集合增加数据的方法
    //              //              listBuffer.append(saleDetailBean)
    //
    //              listBuffer += saleDetailBean
    //
    //            } else {
    //              //todo  orderDetail 集合没有匹配上
    //            }
    //            // todo 5 orderInfo 没关联上的，也就是OptionDetail为None 的，把自己的 id 放在redis缓存中
    //            //    type hash (id,orderinfo)
    //
    //            val jedis: Jedis = RedisUtil.getJedisClient
    //            //            jedis.append(orderId,JSON.toJSONString(orderInfoBean))
    //            jedis.set("orderInfo:" + orderInfoBean.id, JSON.toJSONString(orderInfoBean))
    //
    //
    //          }
    //          }
    //      }
    //    )


    //启动采集器
    ssc.start()
    ssc.awaitTermination()

  }

  private def streamJoinStream(fullOuterJoinResultDstream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))]) = {
    val listBuffer: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]
    fullOuterJoinResultDstream.mapPartitions(
      iter => {
        // TODO 4.1 开始遍历匹配join对象
        val jedis: Jedis = RedisUtil.getJedisClient
        implicit val formater = org.json4s.DefaultFormats
        // for循环中的模式匹配，是在括号里执行，还可以指定值，或者过滤条件 (k, v) <- map if v >= 1
        for ((orderInfoId, (orderInfoOpt, orderDetailOpt)) <- iter) {


          if (orderInfoOpt != None) {
            val orderInfo: OrderInfo = orderInfoOpt.get
            if (orderDetailOpt != None) {
              val orderDetail: OrderDetail = orderDetailOpt.get
              val saleDetail = new SaleDetail(orderInfo, orderDetail)
              listBuffer += saleDetail

            }
            //TODO 4.2 把orderInfo 的数据写一份到redis缓存中,orderInfo id是唯一的
            var orderInfoKey = GmallConstants.FULLOUTERJOIN_REDIS_ORDERINFOKEY_PRE + orderInfoId
            //todo 关键点scala 样例类转成json字符串，不能用我们java里面的json工具，scala识别不了，需要scala专属的json工具才行

            val orderInfoStr: String = Serialization.write(orderInfo)
            //写入过程中同时设定过期时间
            jedis.setex(orderInfoKey, GmallConstants.FULLOUTERJOIN_REDIS_EXPIRE, orderInfoStr)
            //TODO 4.3 读取缓存中的orderdetail 集合来匹配数据
            val order_detailKey = GmallConstants.FULLOUTERJOIN_REDIS_ORDERDETAILKEY_PRE + orderInfoId

            val order_DetailSet: util.Set[String] = jedis.smembers(order_detailKey)
            //todo 4.4 java 的集合对象如果要遍历，需要导入一个java的对象的转换包，不知道如果set为空是否报错
            for (order_detail <- order_DetailSet) {
              val order_detailBean: OrderDetail = JSON.parseObject(order_detail, classOf[OrderDetail])
              val sale_detailBean = new SaleDetail(orderInfo, order_detailBean)
              listBuffer.append(sale_detailBean)
            }
          } else {
            // todo 4.3 orderInfo 没关联上，orderDetail 有值的,
            //  写缓存 type set key orderId value 多个orderDetail,因为一个order_id对应多个orderDetail
            val orderDetail: OrderDetail = orderDetailOpt.get
            val order_detailJson: String = Serialization.write(orderDetail)
            val order_detailKey = GmallConstants.FULLOUTERJOIN_REDIS_ORDERDETAILKEY_PRE + orderDetail.order_id
            jedis.sadd(order_detailKey, order_detailJson)
            //设置key的过期时间
            jedis.expire(order_detailKey, GmallConstants.FULLOUTERJOIN_REDIS_EXPIRE)

            val orderinfoStr: String = jedis.get(GmallConstants.FULLOUTERJOIN_REDIS_ORDERINFOKEY_PRE + orderDetail.order_id)
            if (orderinfoStr != null) {
              val orderinfoBean: OrderInfo = JSON.parseObject(orderinfoStr, classOf[OrderInfo])
              val detail = new SaleDetail(orderinfoBean, orderDetail)
              listBuffer.append(detail)
            }

          }

        }
        //TODO 4.5 关闭redis连接
        jedis.close()
        listBuffer.toIterator
      }
    )
  }
}
