package com.scala.basics.objects

object ObjectDemo1 {
  
  def main(args:Array[String]){
    val user=new User("neo")
    //user.age=20
    print(user)
  }
}

class User(inName:String,inAge:Int=10){
  var name:String = inName
  var age:Int = inAge
  override def toString:String={
    "name="+name+",age="+age
  } 
}

