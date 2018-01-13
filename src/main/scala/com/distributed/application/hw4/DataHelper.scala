package com.distributed.application.hw4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @author Create by xuantang
  * @date on 12/26/17
  */
object DataHelper {
  val FILENAME = "dblp-out.xml"

  val AppName = "DataHelper"
  val Master = "local[32]"
  val ExecutorMemory = "3g"
  val DriverMemory = "2048m"
  val NumExecutors = "10"
  val ExecutorCores = "3"
  val Parallelism = "60"   // num-executors * executor-cores * 2

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    if (args.length < 2) {
      println("You need to input args: [source file] [des file]")
      return
    }

    val sourceFilePath = args(0).toString
    val desFilePath = args(1).toString

    val conf = new SparkConf()
      .setAppName(AppName)

    val sqlContext = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // manually
    val customSchema = StructType(Array(
      StructField("title", StringType, nullable = true),
      StructField("author", ArrayType.apply(StringType), nullable = true),
      StructField("year", IntegerType, nullable = true)))
    // read
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "article")
      .schema(customSchema)
      .load(sourceFilePath)

    df.write.parquet(desFilePath)
    sqlContext.stop()
  }
}
