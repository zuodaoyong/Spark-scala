package com.scala.basics.objects

object ApplyDemo {

  def main(args: Array[String]): Unit = {
     val pig1=Pig("tom")
     println(pig1)
     val pig2=Pig("jack")
    println(pig2)
  }
}

class Pig(pName:String){
   val name=pName
}
object Pig{
  def apply(pName: String): Pig = new Pig(pName)
}