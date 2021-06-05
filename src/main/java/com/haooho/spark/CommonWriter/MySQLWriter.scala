package com.haooho.spark.CommonWriter

import com.haooho.spark.Generator.MySQLGen
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import java.io.{Closeable, Serializable}
import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.reflect.ClassTag
import org.apache.spark.sql.Row
/**
 * Created by haooho-Keon on 2018/11/30
 * mysql 写入工具, 注入sql的方式，基于RDD Action 操作SQL update Statement。 upsert/insert/update/delete 等等..
 */
object MySQLWriter extends Logging {

  /**
   *
   * @param rdd
   * @param writeConfig
   * @param fun
   * @tparam D : 一般都是row,显示提供PreparedStatement
   */
  def MySqlSparkExecute[D: ClassTag](rdd: RDD[D], writeConfig: SparkConf, fun: (D, PreparedStatement) => Unit): Unit = {

    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      val cc = MySQLConn(writeConfig)
      cc.withMySqlClientDo(c => {
        cc.conn.setAutoCommit(false)
        iter.foreach(x => {
          fun(x, c)
          //c.addBatch()
        })
        c.executeBatch()
        cc.conn.commit()
        if (c != null && c.isClosed) c.close()
      })
    })
  }

  /**
   * 隐式提供PreparedStatement执行
   * @param rdd
   * @param writeConfig
   * @param schema
   */
  def MySqlSparkExecute(rdd: RDD[Row], writeConfig: SparkConf, schema : org.apache.spark.sql.types.StructType): Unit = {
    MySqlSparkExecute[Row](rdd,writeConfig,(unit,ps) => {
      val nameAndTypeMap = MySQLGen.mapNameAndType(schema)
      nameAndTypeMap.foreach( x =>{
        val inx = x._1
        val (name,dType) = x._2
        ps.setObject(inx + 1, unit.get(inx), MySQLGen.doMySQLTypeMapping(dType))
      })
      ps.addBatch()
    })
  }

  /**
   * jdbc执行sql
   * @param writeConfig
   */
  def execute(writeConfig: SparkConf): Unit = {
    val sql = writeConfig.get("spark.mysql.sql")
    val ps: PreparedStatement = MySQLConn(writeConfig).buildStatement(writeConfig)
    try{
      ps.execute(sql)
    } finally {
      ps.close()
    }
  }

  case class MySQLConn(writeConfig: SparkConf) extends Logging with Serializable with Closeable {
    var conn: Connection = _

    def withMySqlClientDo[T](ps: PreparedStatement => T): T = {
      try {
        val stat = buildStatement(writeConfig)
        ps(stat)
      }finally {
        close()
      }
    }

    def releaseClient(ps: PreparedStatement): Unit = {
      if (ps != null && ps.isClosed) ps.close()
    }

    def buildStatement(writeConfig: SparkConf): PreparedStatement = {
      if (conn == null || conn.isClosed) {
        Class.forName(writeConfig.get("spark.mysql.driver")).newInstance()
        conn = DriverManager.getConnection(
          writeConfig.get("spark.mysql.url"),
          writeConfig.get("spark.mysql.user"),
          writeConfig.get("spark.mysql.password"))
      }
      conn.prepareStatement(writeConfig.get("spark.mysql.sql"))
    }

    override def close(): Unit = {
      if (conn != null && !conn.isClosed) conn.close()
    }
  }

}


