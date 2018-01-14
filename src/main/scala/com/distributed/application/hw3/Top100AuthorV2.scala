package com.distributed.application.hw3

import java.io.{File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Create by xuantang
  * @date on 12/19/17
  */
object Top100AuthorV2 {

  val FILTER_FIRST = "db/journals/pvldb"
  val FILTER_SECOND = "db/conf/sigmod"
  val FILTER_THIRD = "db/conf/icde"
  // test
  val FILTER_FOURTH = "db/journals/acta"
  val FILTER_FIFTH = "db/journal/acta"

  val AppName = "Top100Keywords"
  val Master = "local[*]"
  val Memory = "spark.executor.memory"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val start = System.currentTimeMillis()
    val conf = new SparkConf()
      .setAppName(AppName)
      .setMaster("local[*]")
    val ss = SparkSession.builder
      .config(conf)
      .getOrCreate()

    // Read data form parquet
    val df = ss.read.parquet("file:///d1/documents/DistributeCompute/dblp-top100author.parquet")

    // gen sql
    val authors = df.filter(df("year") >= 2000 && (df("url").contains(FILTER_FIRST)
      || df("url").contains(FILTER_SECOND) || df("url").contains(FILTER_THIRD)))
      .select("author")

    //authors.show()
    val rdd: RDD[Row] = authors.toJavaRDD.rdd

    //rdd.foreach(line => println(line))

    val rdd1 = rdd.filter(line => line(0) != null).map(line => line(0).toString
      .replace("WrappedArray(", "").replace(")", "")).flatMap(_.split(","))
      .map(p => (p.trim, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)

    rdd1.cache()

    val rdd2 = rdd1.take(100)

    // write to file
    new File("hw3-1552730-db-top100authors.txt").createNewFile()
    val writer = new FileWriter("hw3-1552730-db-top100authors.txt", true)
    rdd2.foreach(author => writer.append(author._1).append(" : " + author._2).append("\n"))
    rdd2.foreach(author => println(author._1 + " : " + author._2))
    writer.close()
    val end = System.currentTimeMillis()
    println(end - start + " ms")
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
      StructField("url", StringType, nullable = true),
      StructField("author", ArrayType.apply(StringType), nullable = true),
      StructField("year", IntegerType, nullable = true)))
    // read
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "article")
      .schema(customSchema)
      .load("/d1/documents/DistributeCompute/dblp-out.xml")
    df.write.parquet("/d1/documents/DistributeCompute/dblp-top100author.parquet")
  }
}
