package com.scala.basics.extendsdemo

/**
  * 继承
  */
object ExtendsDemo1 {
  def main(args: Array[String]): Unit = {
    val s=new Stu
    println(s.name)
    s.test()
  }
}

class Person{
  var aa:String="aa"
  private[extendsdemo] var name:String="bb"
  protected var sal:Int=20
  def test(): Unit ={
      println("Person test")
  }
}

class Stu extends Person {
  def fun(): Unit ={
    print(aa+","+sal)
  }

  override def test(): Unit = {
    super.test()
    println("Stu test")
  }
}
