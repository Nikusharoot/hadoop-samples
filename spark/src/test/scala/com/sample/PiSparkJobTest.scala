package com.sample

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext


class PiSparkJobTest extends FunSuite with SharedSparkContext   {
   test ("pi"){ 
    val arrs:Array[String] = Array("2", "2")
    assert(PiSparkJob.extracted(arrs, sc) >= 3)
  }
}