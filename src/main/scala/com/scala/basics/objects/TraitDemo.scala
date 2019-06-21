package com.scala.basics.objects

object TraitDemo {

  def main(args: Array[String]): Unit = {
    val c=new C()
    c.getConnect()
    val e=new E
    e.getConnect()
  }

}

trait Trait1{
  def getConnect()
}

class A{}
class B extends A{}
class C extends A with Trait1 {
  override def getConnect(): Unit = {
    println("C")
  }
}

class D{}
class E extends D with Trait1 {
  override def getConnect(): Unit = {
    println("E")
  }
}
class F extends D{}