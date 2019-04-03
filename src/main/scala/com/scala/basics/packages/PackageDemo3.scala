
package object com{
   def com_fun(){
     print("com_fun")
   }
}

package com{
  package object scala{
     var name:String="king"  
  }
  
  package scala{
    object PackageDemo3{
      def main(args:Array[String]){
        print(name)
        com_fun()
      }
    }
  }
  
}