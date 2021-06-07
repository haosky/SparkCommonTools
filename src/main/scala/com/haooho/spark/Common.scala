package com.haooho

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

package object Common {

  object SparkLogger extends Logging {
    def setSparkLogLevels() {
      val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
      if (!log4jInitialized) {
        Logger.getRootLogger.setLevel(Level.ERROR)
      }
    }
  }

  object SparkSessionSingleton {

    @transient private var instance: SparkSession = _
    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
