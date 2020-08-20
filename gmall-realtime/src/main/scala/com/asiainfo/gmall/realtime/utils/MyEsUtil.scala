package com.asiainfo.gmall.realtime.utils

import java.util
import java.util.Properties

import com.asiainfo.gmall.common.constants.GmallConstants
import io.searchbox.action.Action
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory, JestResult, JestResultHandler}
import io.searchbox.core.{Bulk, BulkResult, Index}



object MyEsUtil {
  private val prop: Properties = PropertiesUtil.load(GmallConstants.CLASSPATH_CONFIG)
  //指定为null,必须同时指定参数的类型
  private var factory:JestClientFactory = null

  private val ES_SERVER = prop.getProperty("es.host")
  private val ES_PORT: String = prop.getProperty("es.port")


def saveToEsBatch(docList:List[(String,Any)],indexName:String,type_name:String)= {

  if(docList!=null&&docList.size>0) {
    println("index="+indexName+" type_name="+type_name+"调用一次 saveToBatch 方法")
    val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType(type_name)
    val jest: JestClient = GetJestClient

    for ((id, bean) <- docList) {
      //    val index: Index = new Index.Builder(bean).index("stu_index").`type`("stu").id(id).build()
      //一次插入动作
      val indexBuilder = new Index.Builder(bean)
      if (id != null) {
        indexBuilder.id(id)
      }
      val index: Index = indexBuilder.build()
      bulkBuilder.addAction(index)
    }

    //    var items: util.List[BulkResult#BulkResultItem] = null
    //创建的过程就是调用，前面都是设置属性
    val bulk: Bulk = bulkBuilder.build()
    //开始插入es
    var result:BulkResult=null
  try{

    result= jest.execute(bulk)
  }catch {
    case e:Exception=>{
      println("*************************** catch "+jest+" **************************")
      println("====================== exception==========================================================")
      e.printStackTrace()
      println("====================== ==========================================================")
    }

  }
//    jest.close()
    if (result==null) {

    }else{

      var items: util.List[BulkResult#BulkResultItem] = result.getFailedItems
      val total_items: util.List[BulkResult#BulkResultItem] = result.getItems
      import collection.JavaConversions._
      for(item <- items){
        if (item.error!=null &&item.error.nonEmpty){
          println(item.error)
          println(item.errorReason)
        }

      }
      println(s"保存成功，${total_items.size()}失败${items.size()} 条")
    }
  }
}

  def saveEs(): Unit ={

    val jest: JestClient = GetJestClient
    val put: Index = new Index.Builder(Stu("李四",30)).index("stu_index").`type`("stu").id("2").build()
    jest.execute(put)
    //归还连接
//    jest.close()


  }

  private def GetJestClient = {
    if (factory==null) build()
    val jest: JestClient = factory.getObject
    println(jest)
    if(jest==null){
      println("jest 为null,连接数已用完")
    }
    jest
  }

  private def build() = {
    factory = new JestClientFactory
    //建工厂的时候设置一些属性,默认是8个连接
    val hostList = new util.ArrayList[String]()
    hostList.add("http://s102:9200")
    hostList.add("http://s103:9200")
//    hostList.add("s104:9200")

    factory.setHttpClientConfig(new HttpClientConfig.Builder(hostList).multiThreaded(true)
      .maxTotalConnection(20).build())

  }

  def main(args: Array[String]): Unit = {
    saveEs()
  }


}
case class Stu(name:String,age:Int)
