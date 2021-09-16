package com.haooho.spark.Example

import com.haooho.spark.CommonWriter.ClickHouseWriter
import com.haooho.spark.Example.ESReaderExample.imgObject
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object PCASearchExample extends Logging  {

  case class imgV(img_vector: Array[Double],vector: org.apache.spark.ml.linalg.Vector ,goods_id:  java.math.BigDecimal,platform: String,id:String,off:Int,updateTime:java.sql.Timestamp,pow:Double)
  def main(args:Array[String]): Unit ={

    val spark = SparkSession
      .builder()
      .appName("ESReaderExample")
      .master("local[6]")
      .getOrCreate()

    val readData = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://vm:9004/default?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "images_search")
      .option("user", "default")
      .option("password", "")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .load()

    // data.map(_.img_vector)

    readData.printSchema()
    readData.repartition(2000)

    import spark.implicits._

    val cdd = readData.rdd.map( x=>{
      implicit  val formats = DefaultFormats
      val vectors = parse( x.getAs[String]("vectors")).extract[Array[Double]]
      if(vectors.length == 1000)
      {
        val id = x.getAs[String]("id")
        val platform = x.getAs[String]("platform")
        val off = x.getAs[Int]("off")
        val updateTime = x.getAs[java.sql.Timestamp]("updateTime")
        val goods_id = x.getAs[ java.math.BigDecimal]("goods_id")
        val pow = x.getAs[Double]("pow")
        imgV(vectors,Vectors.dense(vectors),goods_id,platform,id,off,updateTime,pow)
      }else{
        null
      }
    }).filter( _!=null).toDF()


    val pca = new PCA()
      .setInputCol("vector")
      .setOutputCol("img_vector_out")
      .setK(5)
      .fit(cdd)

    pca.save("./pca.ml")

    val result = pca.transform(cdd)
    result.show(false)
//
//
//    val projected = cdd.map(p => p.copy(img_vector_out = pca.transform(p.vector)))
//
    val sql =  "INSERT INTO images_search_pca(id,goods_id,pow,platform,off,vectors,out_vectors,out_pow) VALUES (?,?,?,?,?,?,?,?)";
    println(sql)

    val reConfig = spark.sparkContext.getConf.setAll(Map(
      "spark.mysql.url" -> "jdbc:mysql://vm:9004/default?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8",
      "spark.mysql.password" -> "",
      "spark.mysql.user" -> "default",
      "spark.mysql.sql" -> sql,
      "spark.mysql.driver" -> "com.mysql.cj.jdbc.Driver"
    ))


    ClickHouseWriter.MySQLSparkExecute[Row](result.rdd,reConfig,(unit,ps) => {
      val goods_id = unit.getAs[java.math.BigDecimal]("goods_id").toBigInteger
      val platform = unit.getAs[String]("platform")
      val vectors:Array[Double] = unit.getAs[org.apache.spark.ml.linalg.DenseVector]("img_vector_out").toArray
      val id = unit.getAs[String]("id")
      val sp2 = vectors.reduce((x,y) => x+ y *y)
      val powOUt = Math.sqrt(sp2)

      ps.setString(1, id)
      ps.setString(2,""+goods_id)
      ps.setDouble(3,unit.getAs[Double]("pow") )
      ps.setString(4,platform)
      ps.setInt(5,unit.getAs[Integer]("off") )
      ps.setString(6, "["+ unit.getAs[scala.collection.mutable.WrappedArray[Double]]("img_vector") .mkString(",")+"]")
      ps.setString(7, "["+ vectors.mkString(",")+"]")
      ps.setDouble(8, powOUt)
      ps.execute()
    })
    spark.stop()
  }

}