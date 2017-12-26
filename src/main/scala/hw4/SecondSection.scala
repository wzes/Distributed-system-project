package hw4

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Create by xuantang
  * @date on 12/26/17
  */
object SecondSection {
  def main(args: Array[String]): Unit = {
    val appName = "ApplicationTWO"
    val master = "local[*]"
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    //val ssc = new StreamingContext(conf, Seconds(5))
    //val lines = ssc.socketTextStream("192.168.1.109", 9999)
    conf.set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)
    val AUTHOR = "Wim H. Hesselink"

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
      .load("/d1/documents/DistributeCompute/dblp-out.xml")

    //df.show()

    val authors = df.filter(df("year").isNotNull && df("author").cast(StringType).contains(AUTHOR))
    //
    //titles.show()

    val rdd: RDD[Row] = authors.toJavaRDD.rdd

    val rdd1 = rdd.filter(line => line(0) != null).map(line => line(0).toString
      .replace("WrappedArray(", "").replace(")", ""))
    val rdd2 = rdd1.flatMap(_.split(","))
      .map(word => word.trim)
    val rdd3 = rdd2.distinct()

    // transform
    val rdd4 = rdd2.map(author => ((AUTHOR, author), 1))
      .reduceByKey(_ + _).filter(!_._1._2.equals(AUTHOR))
      .sortBy(_._2, ascending = false)

    // count
    println(rdd3.count() - 1)
    rdd4.foreach(au => {
      println(au._1._2 + ": " + au._2)
    })

    //ssc.start()             // Start the computation
    //ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
