package com.scala.basics.packages

import com.scala.basics.packages.subpackage1.{Tiger=>A}//将Tiger起个别名
import com.scala.basics.packages.subpackage2.Tiger

object PackageDemo1 {
  def main(args:Array[String]){
    //当引用相同类时，解决冲突的方式
    //一、
    val tiger1=new com.scala.basics.packages.subpackage1.Tiger
    val tiger2=new com.scala.basics.packages.subpackage2.Tiger
    //二、
    val tiger3=new A
    val tiger4=new com.scala.basics.packages.subpackage2.Tiger
  }
  
}