package com.scala.basics.functions.exercises
/**
 * f(1)=3,f(n)=2*f(n-1)+1
 */
object RecursionDemo1 {
  
  def main(args: Array[String]): Unit = {
    print(fun(3))
  }
  
  def fun(n:Int):Int={
    if(n==1){
      3
    }else{
      2*fun(n-1)+1
    }
  }
}