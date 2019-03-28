package com.scala.basics.functions

object FunctionDemo1 {
  
     def main(args:Array[String]){
       //fun(m="abc")
       fun2(1,2,3,4,5)
     }
     
     def fun(n:Int=1,m:String){
       print(m)
     }
     
     def fun2(n:Int*){
       var sum=0
       for(item<-n){
         sum=sum+item;
       }
       print(sum)
     }
     
}