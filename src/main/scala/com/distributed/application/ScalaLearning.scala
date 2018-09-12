package com.distributed.application

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.bouncycastle.util.test.Test

/**
  *
  * @author Create by xuantang
  * @date on 9/7/18
  */
object ScalaLearning {
  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf()
      .setAppName("First App")
      .setMaster("local[2]")

    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc: SparkContext = ss.sparkContext

    val rdd1: RDD[String] = sc.parallelize(Array("wo", "ni", "men", "wo", "men"), 2)

    val rdd2: RDD[(String, Int)] = rdd1.map(a => (a, 1)).reduceByKey(_ + _)

    rdd2.collect().foreach(println)

    val words = Array("a", "a", "a", "b", "b", "b")

    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD.reduceByKey(_ + _)  //reduceByKey
    val value = wordPairsRDD.groupByKey().map(t => (t._1, t._2.sum))  //groupByKey


    val arr = sc.parallelize(Array(("A", "Sss"),("B", "Sss"),("C", "Sss")))
    arr.flatMap(x => (x._1 + x._2)).foreach(println)

    var pairs = sc.parallelize(Array(("a",0),("b",0),("c",3),("d",6),("e",0),("f",0),("g",3),("h",6)), 2);
    pairs.sortByKey(true, 3).collect().foreach(println)

    pairs.sortBy(_._2).collect().foreach(println)

  }
}
