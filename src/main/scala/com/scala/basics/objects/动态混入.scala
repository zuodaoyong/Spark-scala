package com.scala.basics.objects

object 动态混入 {
  def main(args: Array[String]): Unit = {
    val a=new A1() with T
    a.f1()
    a.f2()
  }
}
trait T{
  def f2(): Unit ={
    println("f2")
  }
}
class A1{
  def f1(): Unit ={
    println("f1")
  }
}
