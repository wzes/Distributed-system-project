package com.distributed.application.hw4

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Create by xuantang
  * @date on 12/26/17
  */
object FirstSection {
  val FILENAME = "/d1/documents/DistributeCompute/dblp-out.xml"
  def main(args: Array[String]): Unit = {
    val appName = "ApplicationOne"
    val master = "local[*]"
    val conf = new SparkConf().setMaster(master).setAppName(appName)

    val ssc = new StreamingContext(conf, Seconds(60))
    val INPUT = ssc.socketTextStream("localhost", 9999).toString

    conf.set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)
    // input
    //val INPUT = "author: Wim H. Hesselink"
    val AUTHOR = INPUT.substring(INPUT.indexOf("author:") + 7).trim

    val sqlContext = new SQLContext(sc)
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

    //df.persist()

    val titles = df.filter(df("author").cast(StringType).contains(AUTHOR))
      .filter(df("year").isNotNull).orderBy(-df("year")).select("title")
    //
    //println(titles.count())

    // stdout
    println(titles.count())
    // persist
    titles.persist()

    titles.foreach(row => {
      println(row(0).toString)
    })

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
