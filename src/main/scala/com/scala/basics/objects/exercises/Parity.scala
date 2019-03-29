package com.scala.basics.objects.exercises
/**
 * 判断一个数的奇偶
 */
object Parity {
   def main(args:Array[String]){
     val parity=new Parity
     println(parity.fun(1))
     println(parity.fun(4))
   }
}

class Parity{
  def fun(n:Int):String={
    if(n%2==0){
      "even"
    }else{
      "odd"
    }
  }
}