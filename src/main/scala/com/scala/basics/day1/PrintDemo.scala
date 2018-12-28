package com.scala.basics.day1

object PrintDemo {

  def main(args: Array[String]): Unit = {
    var str1: String = "hello"
    var str2: String = "world"
    print(str1 + str2)
    var name = "neo"
    var age: Int = 10
    var sal: Float = 10.67f
    var height: Double = 180.15
    printf("name=%s age=%d sal=%.2f height=%.2f", name, age, sal, height)
    print("\n")
    printf(s"name=$name age=$age sal=$sal")

  }
}
