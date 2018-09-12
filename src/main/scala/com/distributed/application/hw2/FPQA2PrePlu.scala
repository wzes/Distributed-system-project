package com.distributed.application.hw2

import java.io.{BufferedWriter, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * @author Create by xuantang
  * @date on 4/22/18
  */
object FPQA2PrePlu {
  val AppName = "FPQA2PrePlu"
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
    val data = ss.sparkContext.textFile("data/nor_trade_" + dataset + "pluno_b.txt")

    val train = ss.sparkContext.textFile("data/nor_trade_" + dataset + "train_pluno_b.txt")

    val test = ss.sparkContext.textFile("data/nor_trade_" + dataset + "test_pluno_b.txt")

    val trainLines = train.collect()

    val testLines = test.collect()

    // process data
    val transactions: RDD[Array[String]] = data.map(s => s.trim.replace(" ", "").split(','))
    val count = transactions.count()
    transactions.cache()
    val arr =  Array(4)
    for (minSupport <- arr) {
      // cal time
      val start = System.currentTimeMillis()
      // calculate
      freqItems(transactions, minSupport, 0.1f, count, trainLines, testLines)

      val end = System.currentTimeMillis()
      println("Cost: " + (end - start) + " ms")
    }


  }


  /**
    *
    * @param transactions
    * @param minSupport
    * @param minConfidence
    */
  def freqItems(transactions: RDD[Array[String]], minSupport: Int, minConfidence: Float, count: Long,
                trainLines: Array[String], testLines: Array[String]): Unit = {

    val fpg = new FPGrowth()
      .setMinSupport(minSupport / count.toDouble)
      .setNumPartitions(1)
    val model = fpg.run(transactions)

    val sortedSingles: Array[FPGrowth.FreqItemset[String]] = model.freqItemsets.collect().filter(freq => freq.items.length < 2).sortWith((left, right) => left.freq > right.freq)

    val singleSortedFreqs: Array[String] = sortedSingles.map(row => row.items.mkString(" "))

    // print items which the confidence more than minConfidence
    val confidenceModel: Array[AssociationRules.Rule[String]] = model.generateAssociationRules(minConfidence).collect()
    // pres
    val pres: Array[String] = confidenceModel.map(rule => rule.antecedent.mkString(" ") + " " + rule.consequent.mkString(" "))
    // predict
    predict(pres, trainLines, testLines, singleSortedFreqs)
  }

  def predict(arrays: Array[String], trainLines: Array[String], testLines: Array[String], singleSortedFreqs: Array[String]): Unit = {
    var rates: Array[Double] = Array()
    for (index <- trainLines.indices) {
      var testCount = 0
      var predictItems: List[String] = List()
      arrays.foreach { freqSequence =>
        // train item
        val train: String = trainLines(index)
        val trainRecords: Array[String] = train.replace(";", ",").replace(" ", "").split(",")
        // test item
        val test: String = testLines(index)
        val testRecords: Array[String] = test.replace(";", ",").replace(" ", "").split(",")
        testCount = testRecords.length
        val freqItems: Array[String] = freqSequence.split(" ")
        freqItems.foreach(pro => {
          if (trainRecords.contains(pro)) {
            freqItems.foreach(preItem => {
              if (!preItem.equals(pro) && testRecords.contains(preItem)
                && !predictItems.contains(preItem)) {
                predictItems = predictItems :+ preItem
              }
            })
          }
        })
        if (predictItems.lengthCompare(testCount) < 0) {
          var count = 0
          val predictCount: Int = testCount - predictItems.size
          for (i <- singleSortedFreqs.indices) {
            // for the vip predict test count product
            if (count < predictCount) {
              if (!predictItems.contains(singleSortedFreqs(i))) {
                if (testRecords.contains(singleSortedFreqs(i))) {
                  predictItems = predictItems :+ singleSortedFreqs(i)
                }
                count += 1
              }
            }
          }
        }
      }
      // print the correct rate
      val rate: Double = predictItems.size / testCount.toDouble
      rates = rates :+ rate
      println("Vip " + index + " correct rate:" + (rate * 100 ).formatted("%.2f") + "%")
    }
    // print average correct rate
    println("Average correct rate: " + ((rates.sum / rates.length) * 100).formatted("%.2f") + "%")
    // output table
    val writer = new BufferedWriter(new FileWriter("data/nor_pre_" + dataset + "rate_plu.csv"))
    writer.write("vip,plu,correct rate")
    writer.newLine()
    for (i <- rates.indices) {
      writer.write(i + "," + "plu," + (rates(i) * 100).formatted("%.2f"))
      writer.newLine()
    }
    writer.close()
  }
}
