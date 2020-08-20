package com.asiainfo.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.asiainfo.gmall.common.constants.GmallConstants
import com.asiainfo.gmall.realtime.bean.{AlertInfo, EventInfo}
import com.asiainfo.gmall.realtime.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks

object CouponAlertApp {
//  需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品。达到以上要求则产生一条预警日志。
  //todo 1 分析：同一设备，5分钟内不同账号登录并领取优惠券,连续的五分钟，需要窗口
  // todo 2 改为3次及以上，没有浏览商品的行为
//  同一设备，每分钟只记录一次预警,最后结果，再次去重，利用es 的幂等性去重

  def main(args: Array[String]): Unit = {
    //sparkconf
    val conf: SparkConf = new SparkConf().setAppName("coupon alert").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    //TODO 1 从kafka消费数据
    val eventLogDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)
    //todo 2 先开窗，然后再来按照mid分组,由于kafka ConsumerRecord不能被序列化，应该先转换结构，后面再用window
//    val windowDstream: DStream[ConsumerRecord[String, String]] = eventLogDstream.window(Minutes(5))
    //分组转换为k-v数据格式
    //增加了2个字段，一个时间字段，一个小时字段，一个按天的字段,服务器采集了日志，增加了ts字段
    val dateformater = new SimpleDateFormat("yyyy-MM-dd HH")
    val eventInfoBeanDstream: DStream[EventInfo] = eventLogDstream.map(
    record => {
      val jsonString: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])
      val ts: Long = eventInfo.ts
      //现在的需求是把时间戳转换为时间格式
      val timeStr: String = dateformater.format(ts)
      val arrs: Array[String] = timeStr.split(' ')
      eventInfo.logDate = arrs(0)
      eventInfo.logHour = arrs(1)
      eventInfo

    }
    )
    //todo 3 开窗，然后再来按照mid分组
    val eventInfoWindowDstream: DStream[EventInfo] = eventInfoBeanDstream.window(Minutes(5))
    val groupByMidDstream: DStream[(String, Iterable[EventInfo])] = eventInfoWindowDstream.map(
      eventInfo => {

        (eventInfo.mid, eventInfo)
      }).groupByKey()
    //TODO 3 检查同一组内的数据是否需要预警
    //5分钟内，不同账号登录
    //领取优惠券，没有浏览商品
    // 一分钟记录一次预警
    //todo 4 希望得到的告警数据类型，针对的 5分钟内是同一个设备，不同账号
    // mid  	设备id
    //uids	领取优惠券登录过的uid，去重
    //itemIds	优惠券涉及的商品id，不去重啊，很可能是同一个商品？？
    //events  	发生过的行为，不去重，list存放
    //ts	发生预警的时间戳


    val alertCouponBeanDstream: DStream[(Boolean, AlertInfo)] = groupByMidDstream.map {
      case (mid, iter) => {
        var flag = true
        val uids = new util.HashSet[String]()
        val itemIds = new util.HashSet[String]()
        val events = new util.ArrayList[String]()
        Breaks.breakable(

          for (elem <- iter) {
            events.add(elem.evid)
            if (elem.evid == "clickItem") {
              flag = false
              Breaks.break()
            } else if (elem.evid == "coupon") {
              uids.add(elem.uid)
              itemIds.add(elem.itemid)
            }

          }
        )
        if (uids.size < 3) flag = false
        (flag, AlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    }
val filterDstream: DStream[(Boolean, AlertInfo)] = alertCouponBeanDstream.filter(_._1)
//TODO 4 转换数据结构，保存到es中，需要封装成（id,alertInfo）形式，以分钟为单位去重
    val esDataDstream: DStream[(String, AlertInfo)] = filterDstream.map {
      case (flag, alertInfo) => {
        val newId: String = alertInfo.mid + "_" + alertInfo.ts / 1000 / 60
        (newId, alertInfo)

      }
    }
    //需要封装成一个list集合,按照分区来统计就得到多个rdd迭代器
    esDataDstream.foreachRDD(
      rdd=>{
        rdd.foreachPartition(
          //driver执行
          iter=> {
            val list: List[(String, AlertInfo)] = iter.toList
            println("1 type="+GmallConstants.ES_INDEX_COUPON_ALERT_TYPE)
            MyEsUtil.saveToEsBatch(list,GmallConstants.ES_INDEX_COUPON_ALERT,GmallConstants.ES_INDEX_COUPON_ALERT_TYPE)
          }
        )
      }
    )

//    alertCouponDstream.foreachRDD(
//      rdd=> rdd.foreach(println)
//    )

    //启动采集器
    ssc.start()
    ssc.awaitTermination()

  }

}
