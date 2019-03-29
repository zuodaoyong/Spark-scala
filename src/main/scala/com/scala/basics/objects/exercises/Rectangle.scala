package com.scala.basics.objects.exercises
/**
 * 打印10行8列矩形
 */
object Rectangle {
  def main(args:Array[String]){
    val rectangle=new Rectangle
    /*rectangle.fun()
    print(rectangle.calc())*/
    rectangle.printFun(3,3)
    print(rectangle.calcFun(3,3))
  }
}

class Rectangle{
  //带参数求面积
  def calcFun(len:Int,width:Int):String={
    (len*width).toDouble.formatted("%.2f")
  }
  //打印
  def printFun(m:Int,n:Int){
    var i=1
    var j=1
    for(i<-1 to m){
      for(j<-1 to n){
        print("*")
      }
      println()
    }
  }
  
  //计算面积
  def calc():String={
    (8*10).toDouble.formatted("%.2f")
  }
  //打印
  def fun(){
    var i=1
    var j=1
    for(i<-1 to 10){
      for(j<-1 to 8){
        print("*")
      }
      println();
    }
  }
}