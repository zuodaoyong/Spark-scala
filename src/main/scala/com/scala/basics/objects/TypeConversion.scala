package com.scala.basics.objects

/**
  * 类型转换
  */
object TypeConversion {

  def main(args: Array[String]): Unit = {
      var person1=new Person1
      var employ1:Person1=new Employ1
      println(classOf[Person1])
      println(person1.getClass)
      println(person1.isInstanceOf[Person1])
      var e=employ1.asInstanceOf[Employ1]
      println(e)
      println(person1)
  }
}

class Person1{
   def b(): Unit ={

   }
}

class Employ1 extends Person1 {
  def a(): Unit ={

  }
}
