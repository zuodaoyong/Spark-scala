package main.scala.com.scala.basics.packages

object PackageDemo4 {

  def main(args: Array[String]): Unit = {
    val c=new Clerk
    print(c.name)
  }
}

class Clerk{
  //当为private时 本包的内的其他类无法访问，但是用private[packages]修饰时，则其他类可以访问
  private[packages] var name:String="order"
  def showInfo(): Unit ={
    print(name)
  }
}

object Clerk{
  def test(c:Clerk): Unit ={

  }
}