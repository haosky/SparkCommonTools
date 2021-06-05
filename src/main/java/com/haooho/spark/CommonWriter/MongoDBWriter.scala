package com.haooho.spark.CommonWriter

import com.mongodb.BasicDBObject
import com.mongodb.client.model.UpdateOptions
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.rdd.RDD
import org.bson.Document
import scala.reflect.ClassTag

/**
  * Created by Keon on 2018/11/30
  * mysql 插入、 更新 写入工具
  */
object MongoDBWriter{

  def MongoSparkUpsert[D: ClassTag](rdd: RDD[D], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      val uo: UpdateOptions = new UpdateOptions()
      uo.upsert(true)
      val sk = "_id"
      mongoConnector.withMongoClientDo(c => {
        iter.foreach(
          x => {
            val doc = x.asInstanceOf[Document]
            val documentNew: Document = new Document()
            documentNew.append("$set", doc)
            c.getDatabase(writeConfig.databaseName).getCollection(writeConfig.collectionName).updateOne(new BasicDBObject(sk, doc.get(sk)), documentNew, uo)
          }
        )
      })
    })
  }

}


