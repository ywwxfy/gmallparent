package com.asiainfo.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.asiainfo.gmall.common.constants.GmallConstants
import com.asiainfo.gmall.realtime.bean.OrderInfo
import com.asiainfo.gmall.realtime.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("orderApp")
    //拿到一个streamingContext 对象
    val ssc = new StreamingContext(conf,Seconds(5))
    val sc: SparkContext = ssc.sparkContext
    import org.apache.phoenix.spark._

    //实时消费kafka中的数据，然后保存到hbase中
    //TODO 2 把create_time字段拆分得到两个时间字段
    //TODO 3  consignee_tel 联系人电话脱敏 173****1282

    val orderDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_INFO,ssc)
    println("1 开始转换流数据")
    val beanStream: DStream[OrderInfo] = orderDstream.map(record => {
      //executor中执行
      val value: String = record.value()
      JSON.parseObject(value, classOf[OrderInfo])
    })
//    val OrderInfoBeanDstream: _root_.org.apache.spark.streaming.dstream.DStream[_root_.com.asiainfo.gmall.realtime.bean.OrderInfo] = hadleStream(beanStream)
    val OrderInfoBeanDstream: DStream[OrderInfo] = hadleStream(beanStream)
    println("2 保存数据到phoenix")
    OrderInfoBeanDstream.foreachRDD(

      rdd=>{
        //代码在driver端执行
        rdd.saveToPhoenix(GmallConstants.PHOENIX_ORDER_INFO,
          Seq("id","province_id", "consignee", "order_comment", "consignee_tel", "order_status", "payment_way", "user_id","img_url", "total_amount", "expire_time", "delivery_address", "create_time","operate_time","tracking_no","parent_order_id","out_trade_no", "trade_body", "create_date", "create_hour"),
          new Configuration(),Some(GmallConstants.ZKURL)
        )
      }
    )

  println("启动")
    //采集器一直工作
    ssc.start()
    ssc.awaitTermination()

  }

  //处理数据的逻辑
  private def hadleStream(beanStream: DStream[OrderInfo]) = {
    val OrderInfoBeanDstream: DStream[OrderInfo] = beanStream.map {
      case orderinfo => {
        //2020-08-08 07:28:40
        val create_time: String = orderinfo.create_time
        val array: Array[String] = create_time.split(' ')
        val hourArray: Array[String] = array(1).split(':')
        orderinfo.create_date = array(0)
        orderinfo.create_hour = hourArray(0)
        val consignee_tel: String = orderinfo.consignee_tel
        //136****
        val before3After8: (String, String) = consignee_tel.splitAt(3)
        val before3After4: (String, String) = consignee_tel.splitAt(7)
        var newTel = before3After8._1 + "****" + before3After4._2
        orderinfo.consignee_tel = newTel
        orderinfo
      }
    }
    OrderInfoBeanDstream
  }
  //todo 2 增加一个订单，得到首次下单的用户
}
