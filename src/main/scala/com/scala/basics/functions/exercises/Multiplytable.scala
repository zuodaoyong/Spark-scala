package com.scala.basics.functions.exercises
/**
 * 9X9 乘法表
 */
object Multiplytable {
  
    def main(args:Array[String]){
      fun(5)
    }
    
    def fun(n:Int){
      var i=1
      var j=1
      for(i<-1 to n){
        for(j<-1 to i){
          print(j+"x"+i+"="+(i*j))
          print(" ")
        }
        println();
      }
    }
}