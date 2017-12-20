import java.io.{File, FileWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Create by xuantang
  * @date on 12/19/17
  */
object Top100Author {

  val FILTER_FIRST = "db/journals/pvldb"
  val FILTER_SECOND = "db/conf/sigmod"
  val FILTER_THIRD = "db/conf/icde"
  val FILTER_FOURTH = "db/journals/acta"
  val FILTER_FIFTH = "db/journal/acta"

  def main(args: Array[String]): Unit = {
    val appName = "Application"
    val master = "local[*]"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    // read
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "article")
      .load("/d1/documents/DistributeComputer/dblp.xml")

    // gen sql
    val authors = df.filter(df("url").contains(FILTER_FIRST)
      || df("url").contains(FILTER_SECOND) || df("url").contains(FILTER_THIRD))
      .filter(df("year") >= 2000).select("author")

    //authors.show()
    val rdd: RDD[Row] = authors.toJavaRDD.rdd

    //rdd.foreach(line => println(line))

    val rdd1 = rdd.filter(line => line(0) != null).map(line => line(0).toString()
      .replace("WrappedArray(", "").replace(")", "").replace("[", "").replace("]", ""))
      .flatMap(_.split(",")).filter(line => !line.equals("null")).map(p => (p.trim, 1))
      .reduceByKey(_ + _).sortBy(_._2, false).take(100)

    // write to file
    new File("hw3-1552730-db-top100authors.txt").createNewFile()
    val writer = new FileWriter("hw3-1552730-db-top100authors.txt", true)
    rdd1.foreach(author => writer.append(author._1).append("\n"))
    rdd1.foreach(author => println(author._1))
    writer.close()
  }
}