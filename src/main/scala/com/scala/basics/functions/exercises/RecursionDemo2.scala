package com.scala.basics.functions.exercises
/**
 * 有一堆桃子，猴子第一天吃了其中一半，并再多吃一个。
 * 以后每天猴子都吃其中一半，然后再多吃一个。当到第十天时，
 * 想再吃（还没吃），发现只有一个1桃子。
 * 问，最初有多少个桃子
 */
object RecursionDemo2 {
  
    def main(args: Array[String]): Unit = {
        print(fun(1))
    }
    
    def fun(n:Int):Int={
      if(n==10){
        1
      }else{
        (fun(n+1)+1)*2
      }
    }
}