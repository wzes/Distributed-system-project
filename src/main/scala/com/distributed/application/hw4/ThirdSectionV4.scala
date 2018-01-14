package com.distributed.application.hw4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @author Create by xuantang
  * @date on 12/26/17
  */
object ThirdSectionV4 {
  val FILENAME = "/d1/documents/DistributeCompute/dblp-out.xml"

  val AppName = "User22Third"
  val Master = "local[16]"
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
    val hosts = args(2).toString
    val port = args(3).toInt
    val conf = new SparkConf()
      .setAppName(AppName)
    //  .setMaster(Master)
    //      .set("spark.executor.memory", ExecutorMemory)
    //      .set("spark.driver.memory", DriverMemory)
    //      .set("spark.default.parallelism", Parallelism)

    val ss = SparkSession.builder
      .config(conf)
      .getOrCreate()

    val ssc = new StreamingContext(ss.sparkContext, Seconds(seconds))

    // clique: Wim H. Hesselink, Hans Ulrich Simon[2]
    val INPUT = ssc.socketTextStream(hosts, port)

    var df:DataFrame = null

    INPUT.foreachRDD(tmp => {
      val strings: Array[String] = tmp.collect()
      if (strings.length > 0) {
        val AUTHOR = strings(0).substring(strings(0).indexOf("clique:") + 7, strings(0).indexOf("[")).trim
        val NUM = strings(0).substring(strings(0).indexOf("[") + 1, strings(0).indexOf("]")).toInt
        // Split
        val AUTHORS = AUTHOR.split(",")
        val spark = SparkSession.builder.config(tmp.sparkContext.getConf).getOrCreate()
        // cached input data
        if (df == null) {
          df = spark.read.parquet(filename.toString)
          df.cache()
        }
        var df2 = df
        AUTHORS.foreach(str => {
          df2 = df2.filter(row => {
            if (row(1) != null) {
              row(1).toString.contains(str.trim)
            } else false
          })
        })
        val authors = df2.orderBy(-df("year")).select("author", "title", "year")
        //authors.show()
        val rdd: RDD[Row] = authors.toJavaRDD.rdd

        val rdd1 = rdd.map(line => line(1) + " : " +
          line(0).toString.replace("WrappedArray(", "").replace(")", ""))
        rdd1.cache()
        // check the cooperator paper num
        if (rdd1.count() >= NUM) {
          println("YES")
          // stdout
          rdd1.collect().foreach(au => {
            println(au)
          })
        } else {
          println("NO")
          rdd1.collect().foreach(au => {
            println(au)
          })
        }
      }
    })
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

  def handleData(): Unit = {
    val conf = new SparkConf()
      .setAppName(AppName)
    val sqlContext = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    // manually
    val customSchema = StructType(Array(
      StructField("title", StringType, nullable = true),
      StructField("author", ArrayType.apply(StringType), nullable = false),
      StructField("year", IntegerType, nullable = true)))

    // read
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "article")
      .schema(customSchema)
      .load(FILENAME)
    df.write.parquet("dblp-hw4.parquet")
  }
}
