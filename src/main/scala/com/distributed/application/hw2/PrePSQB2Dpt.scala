package com.distributed.application.hw2

import java.io.{BufferedWriter, FileWriter}

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
object PrePSQB2Dpt {
  val AppName = "PrePSQB2Dpt"
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

    val data = ss.sparkContext.textFile("data/seq_trade_" + dataset + "dptno_b.txt")

    val train = ss.sparkContext.textFile("data/seq_trade_" + dataset + "train_dptno_b.txt")

    val test = ss.sparkContext.textFile("data/seq_trade_" + dataset + "test_dptno_b.txt")

    val trainLines = train.collect()

    val testLines = test.collect()

    val rdd1: RDD[Array[String]] = data.map(line => line.trim.split(";"))
    val sequences: RDD[Array[Array[String]]] = rdd1.map(row => row.iterator.map(item => item.replace(" ", "")
      .trim.split(",")).toArray)
    val count = sequences.count()
    sequences.cache()
    val arr =  Array(4)
    for (minSupport <- arr) {
      // cal time
      val start = System.currentTimeMillis()
      // calculate
      freqItems(sequences, minSupport, count, trainLines, testLines)

      val end = System.currentTimeMillis()
      println("Cost Time: " + (end - start) + " ms")
    }
  }

  def freqItems(sequences: RDD[Array[Array[String]]], minSupport: Int, count: Long,
                trainLines: Array[String], testLines: Array[String]): Unit = {
    val prefixSpan = new PrefixSpan()
      .setMinSupport(minSupport / count.toDouble)
    val model = prefixSpan.run(sequences)
    val freqSequences = model.freqSequences.collect()

    val arrays: Array[String] = freqSequences.map(freq => freq.sequence).map(row => flat(row))

    val singles: Array[PrefixSpan.FreqSequence[String]] = freqSequences.filter(freq => freq.sequence.length < 2).sortWith((left, right) => left.freq > right.freq)

    val singleSortedFreqs = singles.map(freq => freq.sequence).map(row => flat(row))

    val mutiFreqs: Array[String] = arrays.filter(_.contains(" "))

    predict(mutiFreqs, trainLines, testLines, singleSortedFreqs)
  }

  def flat(row: Array[Array[String]]): String = {
    var arr: Array[String] = Array()
    for (item <- row) {
      arr = arr :+ item.mkString(" ")
    }
    arr.mkString(" ")
  }

  def predict(arrays: Array[String], trainLines: Array[String],
              testLines: Array[String], singleSortedFreqs: Array[String]): Unit = {
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
    val writer = new BufferedWriter(new FileWriter("data/seq_pre_" + dataset + "rate_dpt.csv"))
    writer.write("vip,dpt,correct rate")
    writer.newLine()
    for (i <- rates.indices) {
      writer.write(i + "," + "dpt," + (rates(i) * 100).formatted("%.2f"))
      writer.newLine()
    }
    writer.close()
  }
}
