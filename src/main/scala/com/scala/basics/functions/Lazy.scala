package com.scala.basics.functions


/**
 * 懒加载
 */
object Lazy {
  def main(args:Array[String]){
    lazy val f=fun;//懒加载
    val f1=fun;
    print("ok")
  }
  
  def fun(){
    print("fun")
  }
}