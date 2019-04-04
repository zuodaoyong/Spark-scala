package com.scala.basics.extendsdemo

object ExtendsDemo1 {
  def main(args: Array[String]): Unit = {
    val s=new Stu
    println(s.name)
    s.fun()
  }
}

class Person{
  var aa:String="aa"
  private[extendsdemo] var name:String="bb"
  protected var sal:Int=20
  def test(): Unit ={

  }
}

class Stu extends Person {
  def fun(): Unit ={
    print(aa+","+sal)
  }
}
