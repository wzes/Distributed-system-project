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
object FirstSection {
  def main(args: Array[String]): Unit = {
    val appName = "ApplicationTWO"
    val master = "local[*]"
    val conf = new SparkConf().setMaster(master).setAppName(appName)

    //val ssc = new StreamingContext(conf, Seconds(5))
    //val lines = ssc.socketTextStream("local", 9999)
    conf.set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)
    val author = "Wim H. Hesselink"

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
      .load("/d1/documents/DistributeComputer/dblp-test.xml")

    val titles = df.filter(df("author").cast(StringType).contains(author))
      .filter(df("year").isNotNull).orderBy(-df("year")).select("title")
    //
    val rdd: RDD[Row] = titles.toJavaRDD.rdd
    val rdd1 = rdd.map(line => line(0))

    // stdout
    println(titles.count())
    titles.foreach(row => {
      println(row(0).toString)
    })
    rdd1.collect()
    //ssc.start()             // Start the computation
    //ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
