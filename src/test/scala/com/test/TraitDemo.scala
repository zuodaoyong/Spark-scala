package com.test

object TraitDemo {
  def main(args: Array[String]): Unit = {
    val b= new BC
    b.fun1()
    b.fun2()
  }
}

class BC extends A{
  override def fun1(): Unit ={
    println("子类fun1")
  }
}

trait A{
  def fun1()
  def fun2(): Unit ={
    println("fun2")
  }
}

