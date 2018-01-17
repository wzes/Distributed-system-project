package com.distributed.application.hw4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @author Create by xuantang
  * @date on 12/26/17
  */
object FirstSectionV4 {
  val FILENAME = "dblp-out.xml"

  val AppName = "FirstSection"
  val Master = "local[*]"
  val ExecutorMemory = "3g"
  val DriverMemory = "2048m"
  val NumExecutors = "10"
  val ExecutorCores = "3"
  val Parallelism = "60"   // num-executors * executor-cores * 2
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    if (args.length < 4) {
      println("You need to input args: [seconds] [filename] [host] [port]")
      return
    }
    val seconds = args(0).toInt
    val filename = args(1).toString
    val host = args(2).toString
    val port = args(3).toInt
    val conf = new SparkConf()
      .setAppName(AppName)
//      .setMaster(Master)
//      .set("spark.executor.memory", ExecutorMemory)
//      .set("spark.driver.memory", DriverMemory)
//      .set("spark.default.parallelism", Parallelism)

    val ss = SparkSession.builder
      .config(conf)
      .getOrCreate()

    val ssc = new StreamingContext(ss.sparkContext, Seconds(seconds))

    val INPUT = ssc.socketTextStream(host, port)

    // var df = null
    var df:DataFrame = null

    INPUT.foreachRDD(rdd => {
      val strings: Array[String] = rdd.collect()
      if (strings.length > 0) {
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        // cached input data
        if (df == null) {
          df = spark.read.parquet(filename)
          df.cache()
        }
        //val df = spark.read.parquet(filename.toString())
        val AUTHOR = strings(0).substring(strings(0).indexOf("author:") + 7).trim

        val df1 = df.filter(row =>
          if (row(1) != null) {
            row(1).toString.toLowerCase.contains(AUTHOR.toLowerCase)
          } else false
        ).orderBy(-df("year")).select("title")
        val rows = df1.collect()
        println("======================================================")
        // output count
        println(rows.length)
        // detail
        rows.foreach(line => {
          println(line(0))
        })
        println("======================================================")
      }
    })
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

  /**
    * //
    * spark-submit --master spark://cluster01:7077 --num-executors 10 --executor-cores 12 --driver-memory 2G --executor-memory 2G --class com.distributed.application.hw4.SecondSectionV3 DC-HW4.jar 1 dblp-hw4.parquet 59.110.136.134 10001
    * //
    *
    * --driver-memory 2G
    * --num-executors 10
    * --executor-memory 2G
    * --executor-cores 12
    * --packages com.databricks:spark-xml_2.11:0.4.1
    * --class com.distributed.application.hw4.FirstSectionV3
    * jar
    * args
    */
}
