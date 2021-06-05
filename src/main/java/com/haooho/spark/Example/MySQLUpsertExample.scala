package com.haooho.spark.Example

import org.apache.spark.sql.{Row, SparkSession}
import com.haooho.spark.CommonWriter.MySQLWriter
import com.haooho.spark.Generator.MySQLGen
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by Keon on 2021/16/04
  * mysql 按主键更新的spark案例
  */
object MySQLUpsertExample {

  def main(args:Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .appName("MysqlUpsertExample")
      .master("local[1]")
      .getOrCreate()

    val table = "tttest111"
    val mysqlDatabaseName = "icecastle"
    val ( outMysqlDriverClass, outUsername, outPassword, outMyqlUrl)  = ("com.mysql.cj.jdbc.Driver", "user", "password", s"jdbc:mysql://1.1.1.1:3306/$mysqlDatabaseName?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true")

    val rddz = spark.sparkContext.parallelize(Seq(Row(1,"a3","1"),Row(2,"bzzz","1"),Row(3,"a43","2"),Row(4,"a","634")))
    val struct =
      StructType(
        StructField("id", IntegerType, true) ::
          StructField("c1", StringType, false) ::
          StructField("c2", StringType, false) :: Nil)

    //  生成Sql
    val sql = MySQLGen.mkUpsertSql(table ,struct)
    println(sql)
    // INSERT INTO testa (id,c1,c2) VALUES (?,?,?) ON DUPLICATE KEY UPDATE  id = values( id), c1 = values( c1), c2 = values( c2)

    val reConfig = spark.sparkContext.getConf.setAll(Map(
      "spark.mysql.url" -> outMyqlUrl,
      "spark.mysql.password" -> outPassword,
      "spark.mysql.user" -> outUsername,
      "spark.mysql.sql" -> sql,
      "spark.mysql.driver" -> outMysqlDriverClass
    ))

    // 隐含式按分区批次执行SQL
    MySQLWriter.MySqlSparkExecute(rddz,reConfig,struct)

    // 显含式按分区批次执行SQL
    val rddData =  spark.sparkContext.parallelize(Seq((1,"a3","1"),(2,"bzzz","1"),(3,"a43","2"),(4,"a","634")))
    MySQLWriter.MySqlSparkExecute[(Int,String,String)](rddData,reConfig,(unit,ps) => {
          ps.setInt(1,unit._1)
          ps.setString(2,unit._2)
          ps.setString(3,unit._3)

          ps.addBatch()
      })
    spark.stop()
  }
}