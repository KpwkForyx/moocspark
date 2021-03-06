package com.imooc.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 使用Spark完成我们的数据清洗操作
 */
object SparkStatCleanJob {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      .master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("/home/xqq/IdeaProjects/ImoocSparkProject/src/resources/output/part-00000")

    accessRDD.take(10).foreach(println)

    //RDD ==> DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    accessDF.printSchema()
    accessDF.show(false)
      accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
        .partitionBy("day").save("/home/xqq/IdeaProjects/ImoocSparkProject/src/resources/clean2")

    spark.stop
  }
}
