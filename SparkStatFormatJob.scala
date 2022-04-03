package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
 * 第一步清洗： 抽取出我们所需要的制定列的数据
 */

object SparkStatFormatJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatFormatJob")
                .master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("file:///data/imooc_data/10000_access.log")

//    access.take(10).foreach(println)
    access.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)

      val time = splits(3) + " " + splits(4)
      var url = splits(11).replaceAll("\"", "")
      val traffic = splits(9)
      if(url == "-"){
        url = "http://www.imooc.com/error/00000"
      }

      DateUtils.parse(time) + " " + url + " " + traffic + " " + ip
    }).saveAsTextFile("/home/xqq/IdeaProjects/ImoocSparkProject/src/resources/output")
    spark.stop()
  }
}

