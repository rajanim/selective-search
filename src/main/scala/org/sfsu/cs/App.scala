package org.sfsu.cs

/**
  * Created by rajanishivarajmaski1 on 10/25/17.
  *
  * Test app.
  */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
  }

}
