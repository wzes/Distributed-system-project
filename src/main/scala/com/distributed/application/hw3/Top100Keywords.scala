package com.distributed.application.hw3

import java.io.{File, FileWriter}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Create by xuantang
  * @date on 12/19/17
  */
object Top100Keywords {

  val FILTER_FIRST = "db/journals/pvldb"
  val FILTER_SECOND = "db/conf/sigmod"
  val FILTER_THIRD = "db/conf/icde"
  // test
  val FILTER_FOURTH = "db/journals/acta"
  val FILTER_FIFTH = "db/journal/acta"

  def main(args: Array[String]): Unit = {
    val appName = "Application"
    val master = "local[*]"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
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
    val conferences = Array("db/journals/pvldb", "db/conf/sigmod", "db/conf/icde")
    // gen sql
    val titles = df.filter(df("year") >= 2000 &&
      conferences.map(x => df("url").contains(x)).reduceLeft(_ || _))
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

    val stopRDD = sc.textFile("stopwords.txt").map(_.toLowerCase)

    val rdd2 = rdd1.subtract(stopRDD).map(p => (p, 1))
      .reduceByKey(_ + _).sortBy(_._2, ascending = false).take(100)


    new File("com.distributed.application.hw3-1552730-db-top100keywords.txt").createNewFile()
    val writer = new FileWriter("com.distributed.application.hw3-1552730-db-top100keywords.txt", true)
    rdd2.foreach(author => writer.append(author._1).append("\n"))
    rdd2.foreach(author => println(author._1))
    writer.close()
  }
  //
  /**
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
