package com.sample

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import Matchers._

class TopMostFrequentlyRDDTest extends FunSuite with SharedSparkContext   {
   test ("getTop10MostFrequentlyPurchasedCategories"){ 
    val result = TopMostFrequentlyRDD.getTop10MostFrequentlyPurchasedCategories(sc, "./src/test/resources/sampled/08/**")
    result.foreach ({println})
    assert(result.size === 7)
    result should contain allOf ("Envelopes , 1", "Paper , 1")
  }
}