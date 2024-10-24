package com.sample

import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest.FunSuite
import org.scalatest.Matchers.contain
import org.scalatest.Matchers.convertToAnyShouldWrapper

import com.holdenkarau.spark.testing.SharedSparkContext



class Top10CountriesByNetMaskDFJob2Test extends FunSuite with SharedSparkContext   {
   
  test ("getTop10MostFrequentlyPurchasedCategories"){ 
    val sqlContext = new TestHiveContext(sc)
    val result = Top10CountriesByNetMaskDFJob.selectTop10CountriesWithHighestMoney(sqlContext, 
        "./src/test/resources/sampled2/08/**",
        "./src/test/resources/addresses2.csv",
        "./src/test/resources/GeoLite2-City-Locations-en2.csv")
    result.show()
//    assert(result.count() === 10)
  //  val listOfResults = result.map(r => s"${r.getString(0)} , ${r.getString(1)} , ${r.getLong(2)}").collect.toList
    //listOfResults should contain allOf ("Pens & Art Supplies , Newell 318 , 1", "Paper , Xerox 223 , 1")
    
  }
}