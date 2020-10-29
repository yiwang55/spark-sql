package com.yiwang.imooclog

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object LogConvertUtil {

  val struct = StructType(Array(
    StructField("url", StringType)
    , StructField("cmsType", StringType)
    , StructField("cmsId", LongType)
    , StructField("flow", LongType)
    , StructField("ip", StringType)
    , StructField("city", StringType)
    , StructField("time", StringType)
    , StructField("day", StringType)
  ))

  /**
   * 每行log string转换成struct输出样式
   *
   * @param log
   */
  def parseLog(log: String) = {
    try {
      val splits = log.split("\t")
      val domain = "http://www.imooc.com/"
      val url = splits(1)
      var cmsType = ""
      var cmsId = 0L
      val cms = url.substring(url.indexOf(domain) + domain.length)
      if (cms.length > 0) {
        cmsType = cms.split("/")(0)
        cmsId = cms.split("/")(1).toLong
      }
      val ip = splits(3)
      val flow = splits(2).toLong
      val time = splits(0)
      val day = splits(0).substring(0, 10).replaceAll("-", "")
      val city = IpUtils.getCity(ip)

      //    StructField("url", StringType)
      //    , StructField("cmsType", StringType)
      //    , StructField("cmsId", LongType)
      //    , StructField("flow", LongType)
      //    , StructField("ip", StringType)
      //    , StructField("city", StringType)
      //    , StructField("time", StringType)
      //    , StructField("day", StringType)
      Row(url, cmsType, cmsId, flow, ip, city, time, day)
    } catch {
      case exception: Exception =>Row(0)
    }
  }
}
