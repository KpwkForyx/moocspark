package com.imooc.log


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
 * TopN统计Spark作业
 */

object TopNStatJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob")
      //调优，使用默认类型
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("/home/xqq/IdeaProjects/ImoocSparkProject/src/resources/clean2")

    accessDF.printSchema()
    accessDF.show(false)

//    videoAccessTopNStat(spark, accessDF)
    cityAccessTopNStat(spark, accessDF)

    spark.stop()
  }

  /**
   * 按照地市进行统计TopN课程
   */
  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame): Unit={
    import spark.implicits._

    val cityAccessTopNDF = accessDF.filter($"day" === "20161110" && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId")
      .agg(count("cmsId").as("times"))
      .orderBy($"times".desc)

//    cityAccessTopNDF.show(false)

    //Window函数在spark SQL的使用
    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <= 3") //.show(false)
    top3DF.show(false)

    /**
     * 将统计结果写入到MySQL中
     */
    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")

          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })

        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  /**
   * 最受欢迎的TopN课程
   * @param spark
   * @param accessDF
   */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame) = {
    /**
     * DataFrame的操作
     */
    //导入隐式转换
//    import spark.implicits._
//
//    val videoAccessTopNDF = accessDF.filter($"day" === "20161110" && $"cmsType" === "video")
//      .groupBy("day", "cmsId").agg(count("cmsId").as("times ")).orderBy($"times".desc)
//
//    videoAccessTopNDF.show(false)

    /**
     * 使用SQL的方式进行统计
     */
    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopNDF = spark.sql("select day, cmsId, count(1) as times from access_logs " +
      "where day='20161110'" +
      "group by day, cmsId order by times desc")
    videoAccessTopNDF.show(false)

    /**
     * 将统计结果写入到MySQL中
     */
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

}
