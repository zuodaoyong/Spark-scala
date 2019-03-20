package com.scala.basics

import scala.io.StdIn

object StdInDemo {
  def main(args:Array[String]){
    print("姓名:")
    val name:String=StdIn.readLine()
    print("年龄:")
    val age:Int = StdIn.readInt()
    print("薪水:")
    val sal:Double = StdIn.readDouble()
    printf("姓名:%s,年龄:%d,薪水:%.2f",name,age,sal)
  }
}