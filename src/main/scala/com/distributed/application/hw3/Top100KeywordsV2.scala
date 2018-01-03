package com.distributed.application.hw3

import java.io.{File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Create by xuantang
  **/
object Top100KeywordsV2 {

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
    val conf = new SparkConf()
      .setAppName(AppName)
      .setMaster(Master)
      .set(Memory, "8g")
    val ss = SparkSession.builder
      .config(conf)
      .getOrCreate()
    // manually
    val customSchema = StructType(Array(
      StructField("url", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("year", IntegerType, nullable = true)))

    // read data from parquet
    val df = ss.read.parquet("file:///d1/documents/DistributeCompute/dblp-top100keywords.parquet")
    // gen sql
    val titles = df.filter(df("year") >= 2000 && (df("url").contains(FILTER_FIRST)
      || df("url").contains(FILTER_SECOND) || df("url").contains(FILTER_THIRD)))
      .select("title")

    // to rdd
    val rdd = titles.toJavaRDD.rdd

    //rdd.foreach(line => println(line))

    // rm symbol
    val rdd1 = rdd.filter(line => line(0) != null).map(line => line(0).toString
      .replace(":", "").replace(".", "").replace("null", "").replace("?", "")
      .replace(",", "").replace("/", " "))
      .flatMap(_.split(" ")).map(_.toLowerCase)
      .filter(word => word.length > 0 && !word.equals("-"))
    //
    //rdd1.foreach(line => println(line))

    val stopRDD = ss.sparkContext.textFile("stopwords.txt").map(_.toLowerCase)

    val rdd2 = rdd1.subtract(stopRDD).map(p => (p, 1))
      .reduceByKey(_ + _).sortBy(_._2, ascending = false)

    // cache
    rdd2.cache()
    // take
    val rdd3 = rdd2.take(100)
    // write to data
    new File("hw3-1552730-db-top100keywords.txt").createNewFile()
    val writer = new FileWriter("hw3-1552730-db-top100keywords.txt", true)
    rdd3.foreach(author => writer.append(author._1).append("\n"))
    rdd3.foreach(author => println(author._1 + " : " + author._2))
    writer.close()
  }

  def handleData(): Unit = {
    val conf = new SparkConf()
      .setAppName(AppName)
      .setMaster(Master)
      .set("spark.executor.memory", "8g")
    val sqlContext = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    // manually
    val customSchema = StructType(Array(
      StructField("url", StringType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("year", IntegerType, nullable = true)))
    // read
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "article")
      .schema(customSchema)
      .load("/d1/documents/DistributeCompute/dblp-out.xml")
    df.write.parquet("/d1/documents/DistributeCompute/dblp-top100keywords.parquet")
  }

  /**
    * Spark Shell Script
    * spark-shell --packages com.databricks:spark-xml_2.11:0.4.1 --master spark://148.100.92.156:4477
    * import org.apache.spark.sql.SQLContext
    * import com.databricks.spark.xml._
    * val sqlContext = new SQLContext(sc)
    * val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "article").load("dblp.xml")
    * val titles = df.filter(df("url").contains("db/journals/pvldb") || df("url").contains("db/conf/sigmod") || df("url").contains("db/conf/icde")).filter(df("year") >= 2000).select("title")
    * val rdd = titles.toJavaRDD.rdd
    * rdd.foreach(line => println(line))
    */
}
