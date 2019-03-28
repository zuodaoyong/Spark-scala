package com.scala.basics.functions.exercises
/**
 * 斐波拉契
 */
object Fibonacci {
  
     def main(args: Array[String]): Unit = {
       print(fibonacci(7))
     }
     
     def fibonacci(n:Int):Int={
        if(n==1||n==2){
           1
        }else{
           fibonacci(n-1)+fibonacci(n-2)
        }
     }
}