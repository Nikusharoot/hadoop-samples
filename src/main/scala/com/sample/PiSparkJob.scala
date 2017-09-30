package com.sample
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.math.random
   
object PiSparkJob {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
             .setAppName("Spark Pi")
    val sparkContext  = new SparkContext(conf)

    val pi = extracted(args, sparkContext)
    println("Pi is roughly " + pi)
    sparkContext.stop()  
  }
  
  def extracted(args: Array[String], sc:SparkContext):Double = {
      val slices = if (args.length > 0) args(0).toInt else 2
      val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
      val count = sc.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y <= 1) 1 else 0
      }.reduce(_ + _)
      
      4.0 * count / (n - 1)
    }
  
}