package com.sample

import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest.FunSuite
import org.scalatest.Matchers.contain
import org.scalatest.Matchers.convertToAnyShouldWrapper
import java.sql.{Date, DriverManager, SQLException, Timestamp}
import com.holdenkarau.spark.testing.SharedSparkContext


class JDBCWriteTest extends FunSuite with SharedSparkContext   {
  
  test ("selectTop10CountriesWithHighestMoney"){ 
    
    
    
    val sqlContext = new TestHiveContext(sc)
    val result = SimpleCountJob.selectTop10CountriesWithHighestMoney(sqlContext,
        "./src/test/resources/addresses.csv",
        "./src/test/resources/GeoLite2-City-Locations-en.csv",
        "./src/test/resources/sampled/08/**")
    result.show()
    
    SimpleCountJob.extracted(result)
    assert(result.count() === 10)
    val listOfResults = result.map(r => s"${r.getString(0)} , ${r.getString(1)} , ${r.getLong(2)}").collect.toList
    listOfResults should contain allOf ("Pens & Art Supplies , Newell 318 , 1", "Paper , Xerox 223 , 1")
  }
}