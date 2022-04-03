package com.imooc.spark

import java.util.{Date, Locale}
import org.apache.commons.lang3.time.FastDateFormat

/**
 * 日期时间解析工具类：
 * PS：  SimpleDateFormat是线程不安全，解析出的日期会存在错误
 */

object DateUtils {

  //输出日期格式
  val YYYYMMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  //输出日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
   * 获取时间：yyyy-MM-dd HH:mm:ss
   *
   * @param time
   */
  def parse(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
   * 输入日志时间， 类型：Long
   * time: [10/Nov/2016:00:01:02 +0800]
   * @param time
   * @return
   */
  def getTime(time: String): Long ={
    try {
      YYYYMMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1,
        time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }


  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:00:01:02 +0800]"))
  }
}
