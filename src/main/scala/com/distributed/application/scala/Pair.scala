package com.distributed.application.scala

/**
  *
  * @author Create by xuantang
  * @date on 9/7/18
  */
class Pair[T<:Comparable[T]](val first:T, val second:T) {
  def smaller = if (first.compareTo(second) < 0) first else second
}