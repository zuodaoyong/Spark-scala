package com.scala.basics.objects



/**
 * BeanProperty
 */
object beanproperty {
  
   def main(args:Array[String]){
      val bp=new beanproperty("neo",28)
      //print(bp.getAge())
      print(bp.getName()+","+bp.getAge())
   }
}

class beanproperty(inName:String,inAge:Int){
   import scala.beans.BeanProperty
   @BeanProperty 
   var name:String=inName
   
   @BeanProperty
   var age:Int=inAge
}