package com.scala.basics.objects

object BaseConstructor {

  def main(args: Array[String]): Unit = {
     //var emp=new Emp2
     //Person2
    //默认 Person2
    //Emp2
    var emp2=new Emp2("neo")
    //Person2
    //默认 Person2
    //Emp2
    //调用辅助构造器
  }
}

class Person2(inName:String){
  var name=inName
  println("Person2")
  def this(){
    this("调用主构造")
    println("默认 Person2")
  }
}

class Emp2 extends Person2{
  println("Emp2")
  def this(inName:String){
    this
    this.name=inName
    println("调用辅助构造器")
  }
}