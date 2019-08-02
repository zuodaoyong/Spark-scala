package com.test

class Associated(inName:String) {

  var name:String=inName

}

object Associated{

  def apply(): Associated = {
    println("执行了不带参数的apply方法")
    new Associated("匿名")
  }

  def apply(name: String): Associated = {
    println("执行了带参数的apply方法")
    new Associated(name)
  }
}