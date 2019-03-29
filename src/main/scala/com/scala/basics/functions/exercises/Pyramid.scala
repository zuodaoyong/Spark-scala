package com.scala.basics.functions.exercises

import scala.io.StdIn
/**
 * 金字塔
 *  *
   ***
  *****
 *******
*********
 */
object Pyramid {
 
  def main(args:Array[String]){
    fun(5);
  }
  
  def fun(n:Int){
    var i=1
    var j=1
    var k=1
    for(i<-1 to n){
      for(k<-1 to n-i){
        print(" ")
      }
      for(j<-1 to 2*i-1){
        print('*')
      }
      /*for(k<-1 to n-i){
        print(" ")
      }*/
      println()
    }
  }
}