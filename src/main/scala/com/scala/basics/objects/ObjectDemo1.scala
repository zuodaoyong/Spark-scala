package com.scala.basics.objects

object ObjectDemo1 {
  
  def main(args:Array[String]){
    val user=new User("neo")
    //user.age=20
    print(user)
  }
}

class User private(var inName:String,var inAge:Int=10){
  var name:String = inName
  var age:Int = inAge
 
  override def toString:String={
    "name="+name+",age="+age
  }
  println("主构造器")
  def this(inName:String){
    this("",30)
    print("辅助构造器")
  }
}

