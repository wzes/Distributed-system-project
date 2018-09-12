package com.distributed.application

/**
  *
  * @author Create by xuantang
  * @date on 9/7/18
  */
class Calculate(var string: String) {

  def this() = this(null)

  def setString(string: String): Calculate = {
    this.string = string
    this
  }

  def map(f: (String, String) => String) : Calculate = {
    val d = f(string, string)
    new Calculate(d)
  }

//  def map(f: String => String) : String = {
//    val d = f(string)
//    d
//  }

  def filter(f: String => Boolean) : Calculate = {
    val d = f(string)
    if (d) {
      new Calculate("True")
    }
    new Calculate("Flase")
  }

  def collect() : String = {
    this.string
  }



  def out(): Unit = {
    println(string)
  }
}
