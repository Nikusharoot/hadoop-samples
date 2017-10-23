package com.sample

import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest.FunSuite
import org.scalatest.Matchers.contain
import org.scalatest.Matchers.convertToAnyShouldWrapper

import com.holdenkarau.spark.testing.SharedSparkContext


class TopMostFrequentlyDFJobTest extends FunSuite with SharedSparkContext   {
   
  test ("getTop10MostFrequentlyPurchasedCategories"){
    val sqlContext = new TestHiveContext(sc)
    val result = TopMostFrequentlyDFJob.getTop10MostFrequentlyPurchasedProductInEachCategory(sqlContext, "./src/test/resources/sampled/08/**")
    result.show()
    assert(result.count() === 10)
    val listOfResults = result.map(r => s"${r.getString(0)} , ${r.getString(1)} , ${r.getLong(2)}").collect.toList
    listOfResults should contain allOf ("Pens & Art Supplies , Newell 318 , 1", "Paper , Xerox 223 , 1")
  }
}