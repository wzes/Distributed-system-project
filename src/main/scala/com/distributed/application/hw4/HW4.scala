package com.distributed.application.hw4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * @author Create by xuantang
  * @date on 12/26/17
  */
object HW4 {
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
      .setMaster(Master)

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
        // section 1
        if (strings(0).startsWith("author")) {
          printSection1(df, strings(0))
        }
        // section 2
        else if (strings(0).startsWith("coauthor")) {
          printSection2(df, strings(0))
        }
        // section 3
        else if (strings(0).startsWith("clique")) {
          printSection3(df, strings(0))
        }
      }
    })
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }

  /**
    *
    * @param df
    * @param author
    */
  def printSection1(df: DataFrame, author: String): Unit =  {
    val AUTHOR = author.substring(author.indexOf("author:") + 7).trim

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

  /**
    *
    * @param df
    * @param author
    */
  def printSection2(df: DataFrame, coauthor: String): Unit =  {
    val AUTHOR = coauthor.substring(coauthor.indexOf("coauthor:") + 9).trim
    val authors = df.filter(row =>
      if (row(1) != null) {
        row(1).toString.toLowerCase.contains(AUTHOR.toLowerCase)
      } else false
    )
    val rdd: RDD[Row] = authors.toJavaRDD.rdd
    val rdd1 = rdd.map(line => line(1).toString.replace("WrappedArray(", "").replace(")", ""))
    val rdd2 = rdd1.flatMap(_.split(",")).map(word => word.trim).filter(!_.contains(AUTHOR))
    // Cache
    rdd2.cache()
    // transform
    val rdd4 = rdd2.map(author => ((AUTHOR, author), 1))
      .reduceByKey(_ + _).sortBy(_._2, ascending = false)
    // count
    println("======================================================")
    println(rdd2.distinct().count())
    rdd4.collect().foreach(line => {
      println(line._1._2 + " : " + line._2)
    })
    println("======================================================")
  }

  /**
    *
    * @param df
    * @param clique
    */
  def printSection3(df: DataFrame, clique: String): Unit =  {
    val AUTHOR = clique.substring(clique.indexOf("clique:") + 7, clique.indexOf("[")).trim
    val NUM = clique.substring(clique.indexOf("[") + 1, clique.indexOf("]")).toInt
    // Split
    val AUTHORS = AUTHOR.split(",")
    var df2 = df
    AUTHORS.foreach(str => {
      df2 = df2.filter(row => {
        if (row(1) != null) {
          row(1).toString.toLowerCase.contains(str.trim.toLowerCase())
        } else false
      })
    })
    val authors = df2.orderBy(-df("year")).select("author", "title", "year")
    //authors.show()
    val rdd: RDD[Row] = authors.toJavaRDD.rdd

    val rdd1 = rdd.map(line => line(1) + " : " +
      line(0).toString.replace("WrappedArray(", "").replace(")", ""))

    val rows = rdd1.collect()
    // Check the cooperator paper num
    println("======================================================")
    if (rows.length >= NUM) {
      println("YES")
      rows.foreach(au => {
        println(au)
      })
    } else {
      println("NO")
      rows.foreach(au => {
        println(au)
      })
    }
    println("======================================================")
  }
}
