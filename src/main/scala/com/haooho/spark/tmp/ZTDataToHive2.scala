package com.haooho.spark.tmp

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Keon on 2018/12/27
  * 把hbase的数据抽取到Hive ， 不解析Json
    spark-submit  scripts.hbase.HBaseToHive \
      --packages org.apache.hbase:hbase-client:1.1.2,org.apache.hbase:hbase-common:1.1.2,org.apache.hbase:hbase-server:1.1.2 \
      --conf "hbase.zookeeper.quorum=hdp01,hdp02,hdp03,presto.candao"   \
      --master yarn-cluster --executor-cores 2 --num-executors 2 --driver-memory 4G --executor-memory 2G ./bigcandao_2.11-0.1.jar zt_log zt "" "" 1000 actionName,apiName,className,clientIp,clientType,costTIme,errName,flag,ip,isErr,level,logId,logTime,methodName,msg,springAppName,step,thread
  */
object ZTDataToHive2 extends Logging {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("hive.metastore.uris", "thrift://hdp02:9083")
      .appName(this.getClass.getSimpleName)
      .enableHiveSupport()
      .getOrCreate()
    val conf = HBaseConfiguration.create()



//    m.first()
//    val jsond = """{    "thirdPreferKeyWords": [ {   "type": 1,   "childType": 2,   "title": "满20减5",   "content": "满减5",   "price": 5,   "thirdSubsidy": 1,   "merchantSubsidy": 4 }   ]} """
val jsond = """{"accessKey":"d50fc2c94fe2a16c","timestamp":1543170640261,"sign":"7ca0b63153fabf4b18ddfebd6cb1d141","actionName":"candao.order.postOrder","data":{"extId":"2018112511385713735105200153","thirdSn":"153","extStoreId":"2409","shift":"早班","name":"","phone":"","memberId":"","address":"","tableNum":"","userNote":"","type":5,"payType":1,"isPayed":true,"paymentDetails":[{"money":56.0,"type":1,"typeName":"微信支付"}],"totalPrice":56.0,"preferentialPrice":0.0,"merchantPrice":56.0,"thirdPreferKeyWords":[],"peopleNum":1,"orderStatus":100,"products":[{"num":2,"price":3.0,"sharePrice":3.0,"name":"米饭"},{"num":1,"price":35.0,"sharePrice":35.0,"name":"肥西老母鸡汤中份"},{"num":1,"price":12.0,"sharePrice":12.0,"name":"香肠蒸豆米"},{"num":1,"price":6.0,"sharePrice":6.0,"name":"农家蒸蛋"}]}}"""
    var msgStruct =  common.JsonToStruct(data_modules.modules("actionName,flag:candao.order.postOrder,1"))
    implicit val formats = org.json4s.DefaultFormats

    val z3 = common.JsonToRow(jsond, data_modules.modules("actionName,flag:candao.order.postOrder,1"))

    println(z3)

    val ll = new java.util.ArrayList[Row]()
    ll.add(z3)

    val SimpleDataFrame = spark.createDataFrame(ll,  msgStruct)
    SimpleDataFrame.printSchema()
    SimpleDataFrame.show()
//    SimpleDataFrame.printSchema()
//    SimpleDataFrame.show()


    spark.stop()
  }


}
