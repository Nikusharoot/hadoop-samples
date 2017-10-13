package com.sample

import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest.FunSuite
import org.scalatest.Matchers.contain
import org.scalatest.Matchers.convertToAnyShouldWrapper
import java.sql.{ Date, DriverManager, SQLException, Timestamp }
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.SQLContext

class JDBCWriteTest extends FunSuite with SharedSparkContext {

  test("selectTop10CountriesWithHighestMoney") {
    clearDB()
    val sqlContext = new TestHiveContext(sc)
    val result = extracted2(sqlContext)

    result.show()

    SimpleCountJob.extracted(result, "sales8")
    assert(result.count() === 3)
    val listOfResults = readDataFromDB(sqlContext)
    assert(listOfResults.length === 3)
    listOfResults should contain allOf ("Name.1 , 20.1", "Name.5 , 10.4", "Name.6 , 8.6")
  }

  def extracted(sqlContext: org.apache.spark.sql.hive.test.TestHiveContext) = {
    val result = SimpleCountJob.selectTop10CountriesWithHighestMoney(sqlContext,
      "./src/test/resources/addresses.csv",
      "./src/test/resources/GeoLite2-City-Locations-en.csv",
      "./src/test/resources/sampled/08/**")
    result
  }

  def extracted2(sqlContext: org.apache.spark.sql.hive.test.TestHiveContext) = {
    val schema =
      StructType(
        Array(
          StructField("countryName", StringType, true),
          StructField("sumPrice", DoubleType, true)))

    val rowRDD = sc.parallelize(Array(
      Row("Name.1", 20.1),
      Row("Name.5", 10.4),
      Row("Name.6", 8.6)))

    sqlContext.createDataFrame(rowRDD, schema)
  }

  def readDataFromDB(sqlContext: SQLContext): List[String] = {
    val test = sqlContext.read
      .option("url", "jdbc:mysql://localhost:3306/sales/")
      .option("user", "root")
      .option("password", "cloudera")
      .option("dbtable", "sales8")
      .option("driver", "com.mysql.jdbc.Driver")
      .format("jdbc")
      .load()
    test.show()
    test.map(row => { s"${row.getString(0)} , ${row.getDouble(1)}" }).collect.toList
  }

  def clearDB() {

  }
}