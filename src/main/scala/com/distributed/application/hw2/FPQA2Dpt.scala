package com.distributed.application.hw2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * @author Create by xuantang
  * @date on 4/22/18
  */
object FPQA2Dpt {
  val AppName = "FPQA2Dpt"
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

    // read data
    val data = ss.sparkContext.textFile("data/nor_trade_" + dataset + "dptno_b.txt")

    // process data
    val transactions: RDD[Array[String]] = data.map(s => s.trim.replace(" ", "").split(','))
    val count = transactions.count()
    transactions.cache()
    val arr =  Array(2, 4, 6, 8, 10, 12)
    var costArr: Array[Long] = Array()
    for (minSupport <- arr) {
      // cal time
      val start = System.currentTimeMillis()
      // calculate
      freqItems(transactions, minSupport, 0.2f, count)

      val end = System.currentTimeMillis()
      costArr = costArr :+ (end - start)
    }

    costArr.foreach(println(_))


  }


  /**
    *
    * @param transactions
    * @param minSupport
    * @param minConfidence
    */
  def freqItems(transactions: RDD[Array[String]], minSupport: Int, minConfidence: Float, count: Long): Unit = {
    val fpg = new FPGrowth()
      .setMinSupport(minSupport / count.toDouble)
      .setNumPartitions(1)
    val model = fpg.run(transactions)
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    // print items which the confidence more than minConfidence
//    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
//      println(
//        rule.antecedent.mkString("[", ",", "]")
//          + " => " + rule.consequent .mkString("[", ",", "]")
//          + ", " + rule.confidence)
//    }
  }
}
