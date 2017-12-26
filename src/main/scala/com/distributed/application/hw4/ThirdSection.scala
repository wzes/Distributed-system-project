package com.distributed.application.hw4

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Create by xuantang
  * @date on 12/26/17
  */
object ThirdSection {
  val FILENAME = "/d1/documents/DistributeCompute/dblp-out.xml"
  def main(args: Array[String]): Unit = {
    val appName = "ApplicationThree"
    val master = "local[*]"
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    //val ssc = new StreamingContext(conf, Seconds(5))
    //val lines = ssc.socketTextStream("192.168.1.109", 9999)
    conf.set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)
    val AUTHOR = "Wim H. Hesselink, David Dice, Peter A. Buhr"

    val sqlContext = new SQLContext(sc)
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

    val AUTHORS = AUTHOR.split(",")
    val df1 = df.filter(df("year").isNotNull)
    // get
    var df2 = df1
    AUTHORS.foreach(str => {
      df2 = df2.filter(df("author").cast(StringType).contains(str.trim))
    })

    val authors = df2.orderBy(-df("year")).select("author", "title", "year")
    //
    //authors.show()

    val rdd: RDD[Row] = authors.toJavaRDD.rdd

    val rdd1 = rdd.filter(line => line(0) != null).map(line => line(1) + " : " +
      line(0).toString.replace("WrappedArray(", "").replace(")", ""))

    // check the cooperator paper num
    if (authors.count() >= 3) {
      println("YES")
      // stdout
      rdd1.foreach(au => {
        println(au)
      })
    } else {
      println("NO")
      rdd1.foreach(au => {
        println(au)
      })
    }
    //ssc.start()             // Start the computation
    //ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
