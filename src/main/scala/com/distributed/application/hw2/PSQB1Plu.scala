package com.distributed.application.hw2
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.PrefixSpan
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession;
/**
  *
  * @author Create by xuantang
  * @date on 4/23/18
  */
object PSQB1Plu {
  val AppName = "FrequencyMini"
  val Master = "local[*]"
  val Memory = "spark.executor.memory"
  val dataset = "new_"
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setAppName(AppName)
      .setMaster("local[*]")
    val ss = SparkSession.builder
      .config(conf)
      .getOrCreate()

    val data = ss.sparkContext.textFile("data/seq_trade_" + dataset + "pluno_a.txt")

    val rdd1: RDD[Array[String]] = data.map(line => line.trim.split(";"))
    val sequences: RDD[Array[Array[String]]] = rdd1.map(row => row.iterator.map(item => item.trim.split(" ")).toArray)
    val count = sequences.count()
    sequences.cache()
    val arr =  Array(2, 4, 8, 16, 32, 64)
    var costArr: Array[Long] = Array()
    for (minSupport <- arr) {
      // cal time
      val start = System.currentTimeMillis()
      // calculate
      freqItems(sequences, minSupport, count)

      val end = System.currentTimeMillis()
      costArr = costArr :+ (end - start)
    }
    costArr.foreach(println(_))
  }


  def freqItems(sequences: RDD[Array[Array[String]]], minSupport: Int, count: Long): Unit = {

    val prefixSpan = new PrefixSpan()
      .setMinSupport(minSupport / count.toDouble)

    val model = prefixSpan.run(sequences)
    model.freqSequences.collect().foreach { freqSequence =>
      println(
        freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]") +
          ", " + freqSequence.freq)
    }
  }
}
