package com.haooho.spark.Generator

import org.apache.spark.sql.types
import org.apache.spark.sql.types.DataType

import java.sql.{JDBCType, SQLType}

/**
 * Created by Keon on 2021/16/04
 * mysql 辅助生成工具
 */
object MySQLGen {

  /**
   * @param table
   * @param schema : DataFrame StructType from spark  jdbc reading table schema
   * @return
   */
  def mkUpsertSql( table:String, schema : org.apache.spark.sql.types.StructType) : String = {
    val flds = schema.fields
    val ntMap = mapNameAndType(schema)
    val moreSize = ntMap.size
    val nameSets =  flds.map( "`"+_.name+"`")
    val fieldStr = nameSets.mkString(",")
    val valStr =  List.fill(moreSize)("?").mkString(",")
    val kyStr = nameSets.map(x => s" ${x} = values( ${x})").mkString(",")
    val sql = s"INSERT INTO $table (${fieldStr}) " + s"VALUES (${valStr}) " + s"ON DUPLICATE KEY UPDATE ${kyStr}"
    sql
  }

  /**
   * mapping spark Type to MySQL type , with the statement usage
   * @param tType : inpyut Type
   * @return
   */
  def doMySQLTypeMapping( tType : org.apache.spark.sql.types.DataType ): SQLType ={
    if(tType.isInstanceOf[org.apache.spark.sql.types.DecimalType]){
      return JDBCType.DECIMAL
    }

    tType match  {
      case  types.BinaryType              =>  JDBCType.BINARY
      case  types.BooleanType             =>  JDBCType.BOOLEAN
      case  types.DateType                =>  JDBCType.DATE
      case  types.NullType                =>  JDBCType.NULL
      case  types.ByteType                =>  JDBCType.TINYINT
      case  types.DoubleType              =>  JDBCType.DOUBLE
      case  types.FloatType               =>  JDBCType.FLOAT
      case  types.IntegerType             =>  JDBCType.INTEGER
      case  types.LongType                =>  JDBCType.BIGINT
      case  types.ShortType               =>  JDBCType.SMALLINT
      case  types.StringType              =>  JDBCType.LONGVARCHAR
      case  types.TimestampType           =>  JDBCType.TIMESTAMP
      case _                              =>  JDBCType.LONGVARCHAR
    }
  }

  /**
   *
   * @param schema
   * @return
   */
  def mapNameAndType(schema : org.apache.spark.sql.types.StructType): Map[Int ,(String,DataType) ] ={
    val flds = schema.fields
    val nameAndTypeMap = flds.zipWithIndex.map( x=>{ (x._2, (x._1.name, x._1.dataType))}).toMap
    nameAndTypeMap
  }
}
