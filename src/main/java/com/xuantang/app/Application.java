package com.xuantang.app;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class Application {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Application").setMaster("local[2]");
        SparkContext sc = new SparkContext(conf);

        sc.stop();
    }
}
