package com.haooho.spark.Example

import com.haooho.spark.CommonWriter.ClickHouseWriter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse


object ESReaderExample extends Logging  {
  case class imgObject(img_vector: Array[Float],goods_id: String,platform: String)
  def main(args:Array[String]): Unit ={

    val spark = SparkSession
      .builder()
      .appName("ESReaderExample")
      .master("local[4]")
      .getOrCreate()

    val options = Map(
      "pushdown" -> "true",
      "es.nodes.wan.only" -> "true",
//      "es.nodes" -> "172.16.0.82:9200",
      "es.port" -> "9200",
      "es.nodes" -> "vm:9200",
      "spark.es.mapping.date.rich" -> "false",
      "es.read.field.as.array.include" -> "img_vector",
      "es.read.field.include" -> "goods_id,off,platform,update,img_vector",
      "es.field.read.empty.as.null" -> "true",
      "pushdown"-> "true"
    )

    val data =  EsSpark.esJsonRDD(spark.sparkContext,"search_imgs-vec-1",options).map(
      x => {
        try{
          val id = x._1
          val data = x._2
          implicit  val formats = DefaultFormats
          parse(data).extract[imgObject]
        }catch {
          case e: Exception => {
            logError(e.getMessage)
            null
          }
        }
      }
    ).filter(_!=null)

//      .map( x=>{
//
//      imgV( Vectors.dense(x.img_vector),x.goods_id,x.platform,null)
//
//    })
//
//    val pca = new PCA(5).fit(data.map(_.img_vector))
//    import spark.implicits._
//
//    val projected = data.map(p => p.copy(img_vector_out = pca.transform(p.img_vector)))
//    val fdf = projected.toDF()
//    fdf.show(truncate = false)

    val sql =  "INSERT INTO images_search(id,goods_id,pow,platform,off,vectors) VALUES (?,?,?,?,0,?)";
    println(sql)

    val reConfig = spark.sparkContext.getConf.setAll(Map(
      "spark.mysql.url" -> "jdbc:mysql://172.16.0.82:9004/imageV1?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8",
      "spark.mysql.password" -> "",
      "spark.mysql.user" -> "default",
      "spark.mysql.sql" -> sql,
      "spark.mysql.driver" -> "com.mysql.cj.jdbc.Driver"
    ))

    data.repartition(2000)
    ClickHouseWriter.MySQLSparkExecute[imgObject](data,reConfig,(unit,ps) => {
        val goods_id = unit.goods_id
        val platform = unit.platform
        val vectors:Array[Float] = unit.img_vector

        val id = platform + "-" + goods_id
        val sp2 = vectors.reduce((x,y) => x+ y *y)
        val pow = Math.sqrt(sp2)
        ps.setString(1, id)
        ps.setString(2,goods_id)
        ps.setDouble(3,pow)
        ps.setString(4,platform)
        ps.setString(5, "["+ vectors.mkString(",")+"]")
        ps.execute()
      })
    spark.stop()
  }

}