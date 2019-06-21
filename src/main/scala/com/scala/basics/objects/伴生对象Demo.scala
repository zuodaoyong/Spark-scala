package com.scala.basics.objects

object 伴生对象Demo {

  def main(args: Array[String]): Unit = {
    val child1=new Child("tom")
    val child2=new Child("jack")
    Child.joinGame(child1)
    Child.joinGame(child2)
    Child.show()
  }
}

class Child(inName:String){
    val name=inName
}

object Child{
  var totalChildNum:Int=0
  def joinGame(child:Child): Unit ={
    println(child.name+"加入游戏")
    totalChildNum+=1
  }

  def show(): Unit ={
    println("有"+totalChildNum+"个小孩在玩游戏")
  }
}
