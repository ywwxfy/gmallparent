package com.asiainfo.gmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

//最后得到一张宽表，orderinfo和orderDetail集合
case class SaleDetail(
                       var order_detail_id: String = null,
                       var order_id: String = null,
                       var order_status: String = null,
                       var create_time: String = null,
                       var user_id: String = null,
                       var sku_id: String = null,
                       var user_gender: String = null,
                       var user_age: Int = 0,
                       var user_level: String = null,
                       var sku_price: Double = 0D,
                       var sku_name: String = null,
                       var dt: String = null) {

  //必须直接或者间接调用主构造方法
  def this(orderInfo: OrderInfo, orderDetail: OrderDetail){
    this
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)

  }

  def mergeOrderInfo(orderInfo: OrderInfo) = {
    if(orderInfo!=null){

      this.order_id=orderInfo.id
      this.order_status=orderInfo.order_status
      this.create_time=orderInfo.create_time
      this.user_id=orderInfo.user_id
      this.dt=orderInfo.create_date
    }

  }

  def mergeOrderDetail(orderDetail: OrderDetail): Unit = {

    if(orderDetail!=null){
      this.order_detail_id=orderDetail.id
      this.sku_id=orderDetail.sku_id
      this.sku_name=orderDetail.sku_name
      this.sku_price=orderDetail.order_price.toDouble


    }


  }

  def mergeUserInfo(userInfo: UserInfo)={
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    if(userInfo!=null){
      val birthDate: Date = dateFormat.parse(userInfo.birthday)
      val timestamp: Long = birthDate.getTime
      val days: Long = (System.currentTimeMillis()-timestamp)/1000L/60L/60L/24L/365L
//      println(days.toInt)
      this.user_age=days.toInt
      this.user_gender=userInfo.gender
      this.user_level=userInfo.user_level

    }
  }



}