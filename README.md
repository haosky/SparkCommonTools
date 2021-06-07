## Description
By RDD Partition execution
* MySQL Upsert Support / Customize SQL execution
* MongoDB Upsert Support /Customize Document execution
    

## How To Use
```scala worksheet
  val rddz = spark.sparkContext.parallelize(Seq(Row(1,"a3","1"),Row(2,"bzzz","1"),Row(3,"a43","2"),Row(4,"a","634")))
  val struct =
      StructType(
        StructField("id", IntegerType, true) ::
          StructField("c1", StringType, false) ::
          StructField("c2", StringType, false) :: Nil)

    // generate Sql
    val sql = MySQLGen.mkUpsertSql(table ,struct)
    println(sql)
    // INSERT INTO testa (id,c1,c2) VALUES (?,?,?) ON DUPLICATE KEY UPDATE  id = values( id), c1 = values( c1), c2 = values( c2)
    
    // mapping configuration
    val reConfig = spark.sparkContext.getConf.setAll(Map(
      "spark.mysql.url" -> outMyqlUrl,
      "spark.mysql.password" -> outPassword,
      "spark.mysql.user" -> outUsername,
      "spark.mysql.sql" -> sql,
      "spark.mysql.driver" -> outMysqlDriverClass
    ))

    // Implicit execution on partition batch SQL statement
    MySQLWriter.MySQLSparkExecute(rddz,reConfig,struct)

    // Explicit execution on partition batch SQL statement
    val rddData =  spark.sparkContext.parallelize(Seq((1,"a3","1"),(2,"bzzz","1"),(3,"a43","2"),(4,"a","634")))
    MySQLWriter.MySQLSparkExecute[(Int,String,String)](rddData,reConfig,(unit,ps) => {
          ps.setInt(1,unit._1)
          ps.setString(2,unit._2)
          ps.setString(3,unit._3)

          ps.addBatch()
      })
```
## Install

### Maven pom.xml adding SparkCommonTools as a dependency.

```xml
<dependency>
    <groupId>com.haooho.spark</groupId>
    <artifactId>SparkCommonTools</artifactId>
    <version>1.1</version>
</dependency>
```



## Contributing

### Pull requests for new features, bug fixes, and suggestions are welcome!

## License

[MIT](https://github.com/haosky/SparkCommonTools/blob/main/LICENSE)