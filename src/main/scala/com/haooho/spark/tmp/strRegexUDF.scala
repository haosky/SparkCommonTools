package com.haooho.spark

import scala.util.matching.Regex

/**
  * 特殊字符串 处理
  */
package object tmp {

  // 满减活动数值的提取.
  val valueExtract_ManJian: Regex = "[-减](\\d+\\.?\\d*)".r
  val conditionExtract_ManJian: Regex = "满(\\d+\\.?\\d*)元?[-减]".r
  val express_Male: Regex = ".*?(先生).*?".r
  val express_Female: Regex = ".*?(女士).*?".r
  val check_Allnum: Regex = "^\\d+$".r

  // 街道
  val address_city_district: Regex = "(.*?市)?(.*?区)?(.*?镇)?".r
  val address_stree: Regex = "(.*?[路街道线桥])?(.*?号)?".r


  def genderGuess(value: String): String = value match {
    case express_Male(_) => "男"
    case express_Female(_) => "女"
    case _ => "未知"
  }

  /**
    * @author keon
    *         爬虫数据品牌名称去掉多余字符
    * @param value
    * @return
    * @usage val df = spark.sql("select  wmBrandDataRegexReplaceUDF(brandSS(name).brandname) as brandname, brandSS(name).shopname  as shopname,randSS(name) as shopNameAndBrand  from  wmdata.shop_info")
    */
  def wmBrandDataRegexReplaceUDF(value: String): String = {
    value.replaceAll("\\s+|【.*】", "")
  }

  /**
    * @author keon
    *         爬虫数据依据门店名称 重新洗出品牌和门店名
    * @param nameStr
    * @return
    */
  def wmShopDataBrandAStore(nameStr: String): Map[String, String] = {
    var nameSpl: Array[String] = nameStr.trim.split("[\\(（）)]")
    var name: String = null
    var shopname: String = null
    if (nameSpl.length > 1) {
      name = nameSpl(0)
      shopname = nameSpl(1)
    } else {
      name = nameStr.trim
      shopname = name
    }
    Map("brandname" -> name.trim(), "shopname" -> shopname.trim())
  }

  def wmMenuNameClean(nameStr: String): String = {
    nameStr.replaceAll("\\s+|【.*】|\\(.*\\)|/.*|（.*）|(.*）|（.*)", "")
  }
}

