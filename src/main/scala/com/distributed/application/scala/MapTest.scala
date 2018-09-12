package com.distributed.application.scala

import com.distributed.application.Calculate

/**
  *
  * @author Create by xuantang
  * @date on 9/7/18
  */
object MapTest {
  def main(args: Array[String]): Unit = {
    val str: String = new Calculate()
      .setString("Hello ")
      .map(func)
      .filter(s => s.startsWith("hello"))
      .collect()
    println(str)

    val pair = new Pair("sre", "ssre")
    println(pair.smaller)

    val array = Array(1, 2, 3)
    array.foreach(println)

    type S = String
    val newString: S = "Hello"

    val arr = new Array[String](1)
    arr.foreach{println}

    val add: (String, String) => S = (str1: String, str2: String) => str1 + " " + str2

    println(add("hello", "world"))

    def g(f: (String, String) => String, s1: String, s2: String) {
      println(f(s1, s2))
    }

    g(add, "hello", "world")
  }

  def func(a: String, b: String): String = {
    a + "Xuan tang" + b
  }
}
