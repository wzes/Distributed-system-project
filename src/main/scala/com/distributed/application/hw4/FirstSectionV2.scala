package com.distributed.application.hw4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Create by xuantang
  * @date on 12/26/17
  */
object FirstSectionV2 {
  val FILENAME = "/d1/documents/DistributeCompute/dblp-out.xml"

  val AppName = "ApplicationOne"
  val Master = "local[*]"
  val Memory = "spark.executor.memory"

  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setAppName(AppName)
      .setMaster(Master)
      .set(Memory, "8g")
      .set("spark.cleaner.ttl","2000")
      .set("spark.driver.allowMultipleContexts", "true")
    val ss = SparkSession.builder
      .config(conf)
      .getOrCreate()

    // Read data form parquet
    val df = ss.read.parquet("file:///d1/documents/DistributeCompute/dblp-hw4-test.parquet")

    val rows: Array[Row] = df.collect()

    val ssc = new StreamingContext(conf, Seconds(1))

    val INPUT = ssc.socketTextStream("59.110.136.134", 10001)

    // user broadcast
    val broadcastDF = ssc.sparkContext.broadcast(rows)

    INPUT.foreachRDD(rdd =>
      rdd.foreach { line => {
        val AUTHOR = line.substring(line.indexOf("author:") + 7).trim
        val rows: Array[Row] = broadcastDF.value.filter(row => {
          if (row(1) != null) {
            row(1).toString.contains(AUTHOR)
          } else false
        })
        println(rows.length)
        val tuples: Array[(String, Int)] = rows.map(row => (row(0).toString, Integer.parseInt(row(2).toString)))
        tuples.sortWith(_._2 > _._2).foreach(line => {
          println(line._1 + " : " + line._2)
        })
      }
    })
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

  def handleData(): Unit = {
    val conf = new SparkConf()
      .setAppName(AppName)
      .setMaster(Master)
      .set(Memory, "8g")
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
      .load(FILENAME)
    df.write.parquet("/d1/documents/DistributeCompute/dblp-hw4.parquet")
  }
}
