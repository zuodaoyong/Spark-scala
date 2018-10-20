package com.scala.spark.local.secondarysort

import scala.math.Ordered

class SecondarySortKey(val first:Int, val second:Int) extends Ordered[SecondarySortKey] with Serializable {
  
  def compare(that: SecondarySortKey): Int = {
    if(this.first!=that.first){
      this.first-that.first
    }else{
      this.second-that.second
    }
  }
}