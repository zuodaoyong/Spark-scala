package com.scala.basics.exception

import scala.util.Try

/**
 * 异常
 */
object ExceptionDemo {
   def main(args:Array[String]){
     fun
   }
   
   def fun(){
     val i=0
     val j=1;
     try{
       val res=j/i;
     }catch{
       case ex:ArithmeticException=>print("数字错误")
       case ex:Exception=>print("异常")
     }finally{
       print("运行结束")
     }
   }
}