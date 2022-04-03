package com.imooc.log

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object AccessConvertUtil {

  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )

  /**
   * 根据输入的每一行信息转换成输出的样式
   *
   * @param log
   */

  def parseLog(log: String) = {
    try {
      val splits = log.split(" ")
      //    splits.foreach(println)
      val url = splits(2)
      val traffic = splits(3).toLong
      val ip = splits(4)

      val domain = "http://www.imooc.com/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")
      var cmsType = ""
      var cmsId = 0l
      if (cmsTypeId.length > 1) {
        cmsId = cmsTypeId(1).toLong
        cmsType = cmsTypeId(0)
      }
      val city = IpUtils.getCity(ip)
      val time = splits(0)
      val day = time.substring(0, 10).replaceAll("-", "")

      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    } catch {
      case e: Exception => Row("", "", 0L, 0L, "", "", "", "")
    }
  }
}
