package com.haooho.spark


import com.mongodb.BasicDBObject
import com.mongodb.client.model.UpdateOptions
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.bson.Document
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, Days}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.io.{InputStream, OutputStream}
import java.net.URI
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Locale, Properties}
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created by Keon on 2018/9/11
  *
  ***/
package object tmp {


  def MongoSparkSave[D: ClassTag](rdd: RDD[D], writeConfig: WriteConfig): Unit = {
    val mongoConnector = MongoConnector(writeConfig.asOptions)
    rdd.foreachPartition(iter => if (iter.nonEmpty) {
      var uo: UpdateOptions = new UpdateOptions()
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

  // 获取 根据当天偏移日
  def DateNumOffset(num: Int): Date = {
    val cal = Calendar.getInstance
    val now = new Date
    cal.setTime(now)
    cal.add(Calendar.DATE, num)
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US)
    cal.getTime
  }

  // 获取 根据当天偏移日
  def DateNumOffset(DateStr: String, num: Int): Date = {
    val cal = Calendar.getInstance
    val now = StringToDate(DateStr)
    cal.setTime(now)
    cal.add(Calendar.DATE, num)
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US)
    cal.getTime
  }

  // 获取 根据当天偏移日，与mongodb 时区有关
  def MongodbDateNumOffset(num: Int): Date = {
    val cal = Calendar.getInstance
    val now = new Date
    cal.setTime(now)
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US)
    val sdfz = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss Z", Locale.US)
    cal.add(Calendar.DATE, num)
    sdfz.parse(sdf.format(cal.getTime) + " UTC")
  }

  def StringToDateTime(strDate: String): DateTime = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val dt = formatter.parseDateTime(strDate)
    dt
  }

  def StringToDate(strDate: String): Date = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    val dt = formatter.parse(strDate)
    dt
  }

  def DateToString(date: Date): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    sdf.format(date)
  }

  def DateFromWeekOfStart(dateStr: String): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
    val cal = Calendar.getInstance()
    val d = sdf.parse(dateStr)
    cal.setTime(d)
    cal.setFirstDayOfWeek(Calendar.MONDAY)
    var day = cal.get(Calendar.DAY_OF_WEEK)
    day = day - 1
    if (day == 0) day = 7
    cal.add(Calendar.DATE, -day + 1)
    sdf.format(cal.getTime)
  }

  // 获取日期的前几天
  def DaysBefore(dt: Date, interval: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt)
    cal.add(Calendar.DATE, -interval)
    val before = dateFormat.format(cal.getTime())
    before
  }

  // 获取昨天
  def DaysYesterday(dt: Date): String = {
    DaysBefore(dt, 1)
  }

  def LocalMysqlSettings(user: String = "user", passwd: String = "passwd", url: String = "url"): (String, String, String) = {
    val prop = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("localmysql.properties")
    prop.load(inputStream)
    (prop.getProperty(user), prop.getProperty(passwd), prop.getProperty(url))
  }

  def FineBIMysqlSettings(): (String, String, String) = {
    val prop = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("finebimysql.properties")
    prop.load(inputStream)
    (prop.getProperty("findbi.user"), prop.getProperty("findbi.passwd"), prop.getProperty("findbi.url"))
  }

  def FineReportMysqlSettings(): (String, String, String) = {
    val prop = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("finereportmysql.properties")
    prop.load(inputStream)
    (prop.getProperty("finereport.user"), prop.getProperty("finereport.passwd"), prop.getProperty("finereport.url"))
  }


  def RemoteMongoSettings(): Properties = {
    val prop = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("remotemongo.properties")
    prop.load(inputStream)
    prop
  }

  def LocateMongoSettings(): Properties = {
    val prop = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("localmongo.properties")
    prop.load(inputStream)
    prop
  }

  def getSettings(): Properties = {
    val prop = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("settings.properties")
    prop.load(inputStream)
    prop
  }

  // 计算时间区间，并分群
  def CalcDateInterval(dt1: String, dt2: String, interval: Int): Int = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss")
    val diffInDays = Days.daysBetween(DateTime.parse(dt1, dateFormat), DateTime.parse(dt2, dateFormat))
    val days = diffInDays.getDays
    days - days % interval
  }

  def MysqlProperites(): Properties = {
    val connectionProperties = new Properties()
    val (user, passwd, url) = LocalMysqlSettings()
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    connectionProperties.put("user", user)
    connectionProperties.put("password", passwd)
    connectionProperties.put("url", url)
    connectionProperties
  }


  def MysqlProperites2(user: String = "user", passwd: String = "passwd", url: String = "url"): Properties = {
    val connectionProperties = new Properties()
    val (user1, passwd1, url1) = LocalMysqlSettings(user,passwd,url)
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    connectionProperties.put("user", user1)
    connectionProperties.put("password", passwd1)
    connectionProperties.put("url", url1)
    connectionProperties
  }

  def arrayToStr[T](data: mutable.WrappedArray[T]): String = {
    if (data != null) {
      data.mkString(",")
    }
    else
      ""
  }

  // 获取某个日期所在周的第一天
  def DaysWeekBefore(strDate: String): String = {
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US)
    val cal: Calendar = Calendar.getInstance()
    val dt = formatter.parse(strDate)
    cal.setTime(dt)
    cal.add(Calendar.DATE, -7)
    val before = formatter.format(cal.getTime())
    before
  }

  /**
    * 以闰年为准，生成一年的所有天数
    *
    * @return 字符串数组
    */
  def makeYearDaySeq(start: String = "2020-01-01", end: String = "2020-12-31"): Array[String] = {
    val timeStart = new DateTime(start)
    val timeEnd = new DateTime(end)
    val daysCount = Days.daysBetween(timeStart, timeEnd).getDays()
    val timeSeq = (0 to daysCount).map(timeStart.plusDays(_))
    timeSeq.map(x => "%02d-%02d".format(x.getMonthOfYear, x.getDayOfMonth)).toArray
  }

  /**
    * 根据起止日期，生成中间的所有天数，接收的格式是 yyyy-mm-dd
    *
    * @param start 开始日期
    * @param end   结束日期
    * @return 字符串数组
    */
  def makeYearDaySeqByStartEnd(start: String, end: String): Array[String] = {
    val timeStart = new DateTime(start)
    val timeEnd = new DateTime(end)
    val daysCount = Days.daysBetween(timeStart, timeEnd).getDays()
    val timeSeq = (0 to daysCount).map(timeStart.plusDays(_))
    timeSeq.map(x => x.toString().slice(0, 10)).toArray
  }

  /**
    * 把scan转换成spark 配置可以使用的格式
    *
    */
  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
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

  object StreamingLog extends Logging {

    /** Set reasonable logging levels for streaming if the user has not configured log4j. */
    def setStreamingLogLevels() {
      val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
      if (!log4jInitialized) {
        // We first log something to initialize Spark's default logging, then we override the
        // logging level.
        logInfo("Setting log level to [WARN] for streaming example." +
          " To override add a custom log4j.properties to the classpath.")
        Logger.getRootLogger.setLevel(Level.ERROR)
      }
    }
  }

  val mc = Map(
    "java.lang.Object" -> 1,
    "scala.math.ScalaNumber" -> 2,
    "java.lang.Number" -> 3,
    "scala.collection.immutable.List" -> 4,
    "scala.collection.immutable.AbstractMap" -> 5,
    "scala.collection.immutable.HashMap" -> 5,
    "scala.collection.immutable.ListMap" -> 5
  )
  implicit val formats = org.json4s.DefaultFormats

  def JsonToSchema(strJson: String): String = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = org.json4s.DefaultFormats
    var valueJson: Map[String, Any] = parse(strJson).extract[Map[String, Any]]
    val alp = ListMap(valueJson.toSeq.sortBy(_._1): _*)
    var sb = new StringBuffer()
    alp.foreach(x => {
      sb.append(TypeCatagory(x))
    })
    val middleStr = sb.toString
    s"""{"type": "struct","fields": [$middleStr]}""".replaceAll("},[\\s\n]*\\]", "}]")
  }


  def MapAnalysis(m: (String, Any)): Any = {
    val mz = m._2.asInstanceOf[Map[String, Any]]

    val alp = ListMap(mz.toSeq.sortBy(_._1): _*)
    val k1 = m._1
    var sb = new StringBuffer()
    var rs =
      s"""{  "name": "$k1",
         |     "type": {
         |       "type": "struct",
         |          "fields": [""".stripMargin
    sb.append(rs)
    var rz =
      """     ]
        |      },
        |    "nullable": true,
        |     "metadata": {}
        |},""".stripMargin
    alp.foreach(mk => {
      val fv = TypeCatagory(mk)
      sb.append(fv)
    })

    sb.append(rz)
  }

  def ArrayAnalysis(al: (String, Any)): Any = {
    val lsz = al.asInstanceOf[(String, List[Map[String, Any]])]
    val f1 = lsz._2
    val k1 = al._1
    var rs =
      s"""{
         |"name": "$k1",
         |"type": {
         |"type": "array",
         |"elementType": {
         |"type": "struct",
         |"fields": [""".stripMargin
    var sb = new StringBuffer()
    sb.append(rs)
    if (f1.nonEmpty) {
      // 因为每个Map的字段可能有多有少、所以要对key补全
      var akeysMap = Map[String, Any]()
      f1.foreach(mp => {
        mp.foreach(m => {
          akeysMap += m
        })
      })
      // 按key排序
      val alp = ListMap(akeysMap.toSeq.sortBy(_._1): _*)
      val fv = TypeCatagory(alp)
      sb.append(fv)
    }
    var rz =
      s"""       ]
         |},
         |"containsNull": true
         |},
         |"nullable": true,
         |"metadata": {}
         |},""".stripMargin
    sb.append(rz)
    sb.toString
  }

  def TypeCatagory(ob: (String, Any)): Any = {
    var sb = new StringBuffer()

    val value = ob._2
    val key = ob._1
    val pp = value.getClass.getSuperclass.getTypeName
    val ss = mc(pp) match {
      //      case 1 =>  s""" { "name": "$key",  "type": "string",  "nullable": true,  "metadata": {}  },\n"""
      //      case 2 => s""" { "name": "$key",  "type": "long",  "nullable": true,  "metadata": {}  },\n"""
      //      case 3 => s""" { "name": "$key",  "type": "double",  "nullable": true,  "metadata": {}  },\n"""
      case 1 => s""" { "name": "$key",  "type": "string",  "nullable": true,  "metadata": {}  },\n"""
      case 2 => s""" { "name": "$key",  "type": "string",  "nullable": true,  "metadata": {}  },\n"""
      case 3 => s""" { "name": "$key",  "type": "string",  "nullable": true,  "metadata": {}  },\n"""
      case 4 => ArrayAnalysis(ob)
      case 5 => MapAnalysis(ob)
    }
    sb.append(ss)
    sb
  }

  def TypeCatagory(ob: Map[String, Any]): Any = {
    var sb = new StringBuffer()
    ob.map(x => {
      sb.append(TypeCatagory(x))
    })
    sb.toString
  }

  def JsonToStruct(strJson: String): StructType = {
    import org.apache.spark.sql.types.{DataType, StructType}
    DataType.fromJson(JsonToSchema(strJson)).asInstanceOf[StructType]

  }

  def ZeroM(value: Any): Any = {


    val pp = value.getClass.getSuperclass.getTypeName
    val ss = mc(pp) match {
      case 1 => if (value.isInstanceOf[String] || value.isInstanceOf[Boolean]) "" else 0
      case 2 => 0L
      case 3 => 0.toDouble
      case 4 => ZeroList(value)
      case 5 => ZeroMap(value)
    }
    ss
  }

  def ZeroMap(value: Any): Any = {
    val mz = value.asInstanceOf[Map[String, Any]]
    var resultMap = Map[String, Any]()
    mz.foreach(x => {
      resultMap = resultMap.+(x._1 -> ZeroM(x._2))
    })
    resultMap
  }

  def ZeroList(value: Any): Any = {
    val ml = value.asInstanceOf[List[Map[String, Any]]]
    try {
      var akeysMap = Map[String, Any]()
      ml.foreach(mp => {
        mp.foreach(m => {
          akeysMap += m
        })
      })
      // 按key排序
      val alp = ListMap(akeysMap.toSeq.sortBy(_._1): _*)
      var resultMap = Map[String, Any]()
      alp.foreach(x => {
        resultMap = resultMap.+(x._1 -> ZeroM(x._2))
      })
      List(resultMap)
    } catch {
      case e: java.lang.ClassCastException => {
        //e.printStackTrace()
        List()
      }
    }
  }

  // 初始化的值

  // 补上缺失字段

  def dataCopyValue(value1: Any, value2: Any): Any = {

    val pp = value2.getClass.getSuperclass.getTypeName
    mc(pp) match {
      case 1 => dataCopyR1(value1, value2)
      case 2 => dataCopyR1(value1, value2)
      case 3 => dataCopyR1(value1, value2)
      case 4 => dataCopyList(value1, value2)
      case 5 => dataCopyMap(value1, value2)
    }

  }

  def dataCopyMap(value1: Any, value2: Any): Any = {
    val m1 = value2.asInstanceOf[Map[String, Any]]
    if (value1 == null)
      value2
    else {
      var resultMap = Map[String, Any]()
      val m2 = value1.asInstanceOf[Map[String, Any]]
      m1.foreach(x => {
        val v1 = m2.getOrElse(x._1, x._2)
        val v2 = x._2
        resultMap = resultMap.+(x._1 -> dataCopyValue(v1, v2))
      })
      resultMap
    }
  }

  def dataCopyR1(value1: Any, value2: Any): Any = {
    if (value1 == null)
      value2
    else
      value1
  }

  def dataCopyList(value1: Any, value2: Any): Any = {
    try {
      val ml = value1.asInstanceOf[List[Map[String, Any]]]
      var akeysMap = Map[String, Any]()
      ml.foreach(mp => {
        mp.foreach(m => {
          akeysMap += m
        })
      })
      // 按key排序
      val alp1 = ListMap(akeysMap.toSeq.sortBy(_._1): _*)
      val m2 = value2.asInstanceOf[List[Map[String, Any]]]
      var akeysMap2 = Map[String, Any]()
      m2.foreach(mp => {
        mp.foreach(m => {
          akeysMap2 += m
        })
      })
      // 按key排序
      val alp2 = ListMap(akeysMap2.toSeq.sortBy(_._1): _*)
      var resultList: scala.collection.immutable.List[Map[String, Any]] = scala.collection.immutable.List[Map[String, Any]]()
      ml.foreach(lx => {
        var resultMap: scala.collection.immutable.HashMap[String, Any] = scala.collection.immutable.HashMap[String, Any]()
        alp2.foreach(x => {
          // 如果当前map存在值，取当前的，如果不存在取空值填空的
          resultMap = resultMap.+(x._1 -> alp1.getOrElse(x._1, x._2))
        })
        resultList = resultList ++ scala.collection.immutable.List(resultMap)
      })
      resultList
    } catch {
      case e: java.lang.ClassCastException => {
        //e.printStackTrace()
        value1.asInstanceOf[List[Any]]
      }
    }
  }

  // 补上缺失字段

  // 转换成row
  def mainToRow(value: Any): Any = {
    val pp = value.getClass.getSuperclass.getTypeName
    val ss = mc(pp) match {
      case 1 => value + "" //if(value.isInstanceOf[String] || value.isInstanceOf[Boolean] ) ""  else "0"
      case 2 => value + "" // value.asInstanceOf[scala.math.BigInt].longValue()
      case 3 => value + ""
      case 4 => ListToRow(value)
      case 5 => MapToRow(value)
    }
    ss
  }

  def MapToRow(value: Any): Any = {
    val mv = value.asInstanceOf[Map[String, Any]]
    val alp = ListMap(mv.toSeq.sortBy(_._1): _*)
    Row.fromSeq(alp.map(x => {
      mainToRow(x._2)
    }).toSeq)
  }

  def ListToRow(value: Any): Any = {
    val mv = value.asInstanceOf[Seq[Map[String, Any]]]
    mv.map(x => {
      mainToRow(x)
    })
  }

  def JsonToRow(strJson: String, moduleStrJson: String): Row = {
    var valueJson: Map[String, Any] = parse(strJson).extract[Map[String, Any]]
    var moduleJson: Map[String, Any] = parse(moduleStrJson).extract[Map[String, Any]]
    val zeroModule = ZeroM(moduleJson)
    val fullKeyModule = dataCopyValue(valueJson, zeroModule)
    val rowSets = mainToRow(fullKeyModule)
    rowSets.asInstanceOf[Row]
  }

  object HDataHandler {

    implicit var keyN: Seq[String] = _

    case class HBData(rowkey: String, v: String*)

    implicit def TopLogDataWriter: FieldWriter[HBData] = new FieldWriter[HBData] {
      override def map(data: HBData): HBaseData = {
        val cdvalue = data.v.map(cd => {
          Some(Bytes.toBytes(cd.toString))
        })
        Seq(Some(Bytes.toBytes(data.rowkey))) ++ cdvalue
      }

      override def columns = Seq(keyN: _*)
    }
  }

  /**
    * private static Segment segment;
    * static {
    *     HanLP.Config.IOAdapter = new HadoopFileIoAdapter();
    * segment = new CRFSegment();
    * }
    */
  class HankcsHadoopFileIoAdapter extends IIOAdapter {

    @throws(classOf[java.io.IOException])
    override def open(path: String): InputStream = {
      val conf = new Configuration()
      val fs = FileSystem.get(URI.create(path), conf)
      fs.open(new Path(path))
    }

    @throws(classOf[java.io.IOException])
    override def create(path: String): OutputStream = {
      val conf: Configuration = new Configuration()
      val fs: FileSystem = FileSystem.get(URI.create(path), conf)
      val out: OutputStream = fs.create(new Path(path))
      out
    }
  }

  object HanlpToken {
    HanLP.Config.IOAdapter = new HankcsHadoopFileIoAdapter()

    def hanlpToken(sentence: String): util.List[Term] = {
      val keywordList = HanLP.newSegment.enableAllNamedEntityRecognize(true).seg(sentence)
      keywordList
    }

  }

  def getEnv(key: String = "env") = {
    val prop = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("env.conf")
    prop.load(inputStream)
    prop.getProperty(key,"prod")
  }


  /**val jsondata = "{\"rowkey\":\"" + Bytes.toString(result.getRow) + "\"," + hmsg.substring(hmsg.indexOf("{") + 1)
        logger.debug(jsondata)
        common.JsonToRow(jsondata, msgModule)
  **/
}

