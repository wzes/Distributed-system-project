package com.distributed.application.hw4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Create by xuantang
  * @date on 12/26/17
  */
object ThirdSectionV2 {
  val FILENAME = "/d1/documents/DistributeCompute/dblp-out.xml"

  val AppName = "ApplicationThree"
  val Master = "local[*]"
  val Memory = "spark.executor.memory"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
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

    // clique: Wim H. Hesselink, Hans Ulrich Simon[2]
    val INPUT = ssc.socketTextStream("59.110.136.134", 10001)
    // user broadcast
    val broadcastDF = ssc.sparkContext.broadcast(rows)

    INPUT.foreachRDD(rdd =>
      rdd.foreach { line => {
        // Get author
        val AUTHOR = line.substring(line.indexOf("clique:") + 7, line.indexOf("[")).trim
        val NUM = line.substring(line.indexOf("[") + 1, line.indexOf("]")).toInt
        // Split
        val AUTHORS = AUTHOR.split(",")
        // Filter
        var df2 = broadcastDF.value
        AUTHORS.foreach(str => {
          df2 = df2.filter(row => {
            if (row(1) != null) {
              row(1).toString.contains(str.trim)
            } else false
          })
        })
        val rows = df2
        val rows1: Array[(String, String, Int)] = rows.map(line => (line(0).toString,
          line(1).toString.replace("WrappedArray(", "").replace(")", ""), Integer.parseInt(line(2).toString)))
        val tuples: Array[(String, String, Int)] = rows1.sortWith(_._3 > _._3)

        // Output
        if (tuples.length >= NUM) {
          println("YES")
          // stdout
          tuples.foreach(au => {
            println(au._1 + " " + au._2)
          })
        } else {
          println("NO")
          tuples.foreach(au => {
            println(au._1 + " " + au._2)
          })
        }
      }
      })
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

//    // Read data form parquet
//    val df = ss.read.parquet("file:///d1/documents/DistributeCompute/dblp-hw4-test.parquet")
//
//    //val ssc = new StreamingContext(conf, Seconds(5))
//    //val INPUT = ssc.socketTextStream("localhost", 9999).toString
//
//    // input
//    val INPUT = "clique: Wim H. Hesselink, David Dice, Peter A. Buhr[5]"
//    val AUTHOR = INPUT.substring(INPUT.indexOf("clique:") + 7, INPUT.indexOf("[")).trim
//
//    val NUM = INPUT.substring(INPUT.indexOf("[") + 1, INPUT.indexOf("]"))
//
//    val AUTHORS = AUTHOR.split(",")
//    val df1 = df.filter(df("year").isNotNull)
//    // get
//    var df2 : Dataset[Row] = df1
//
//    AUTHORS.foreach(str => {
//      df2 = df2.filter(row => {
//        if(row(1) != null) {
//          row(1).toString.contains(str.trim)
//        } else false
//      })
//      //df2 = df2.filter(df("author").cast(StringType).contains(str.trim))
//    })
//
//    val authors = df2.orderBy(-df("year")).select("author", "title", "year")
//    //
//    //authors.show()
//
//    val rdd: RDD[Row] = authors.toJavaRDD.rdd
//
//    val rdd1 = rdd.filter(line => line(0) != null).map(line => line(1) + " : " +
//      line(0).toString.replace("WrappedArray(", "").replace(")", ""))
//    // Cache
//    rdd1.cache()
//    // check the cooperator paper num
//    if (rdd1.count() >= 3) {
//      println("YES")
//      // stdout
//      rdd1.foreach(au => {
//        println(au)
//      })
//    } else {
//      println("NO")
//      rdd1.foreach(au => {
//        println(au)
//      })
//    }
    //ssc.start()             // Start the computation
    //ssc.awaitTermination()  // Wait for the computation to terminate
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
      StructField("author", ArrayType.apply(StringType), nullable = false),
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
