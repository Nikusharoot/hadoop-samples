package com.sample
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{ rowNumber, max, broadcast }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

import org.apache.spark.sql.types.{ TimestampType, StructType, StructField, StringType, DoubleType }

object TopMostFrequentlyRDD {

  def getTop10MostFrequentlyPurchasedCategories(sc: SparkContext, src: String): List[String] = {

    val sqlContext = new SQLContext(sc)
    val csvDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .load(src)
    val csvRDD = csvDf.rdd

    csvRDD.map(row => (row.getString(3), 1L))
      .reduceByKey(_ + _).sortBy(_._2, false).take(10).map(item => { s"${item._1} , ${item._2}" }).toList
  }

  def getTop10MostFrequentlyPurchasedProductInEachCategory(sc: SparkContext, src: String): List[String] = {

    /*    val customSchema = StructType(Array(
      StructField("datetime", TimestampType, true),
      StructField("product", StringType, true),
      StructField("price", DoubleType, true),
      StructField("category", StringType, true),
      StructField("ipaddress", StringType, true)))
*/
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val csvDf = sqlContext.read
      .format("com.databricks.spark.csv")
//      .schema(customSchema)
      .option("inferSchema", "true")
      .load(src)

    val csvRDD = csvDf.rdd

    val result = csvRDD.map(row => ((row.getString(3), row.getString(1)), 1L))
      .reduceByKey(_ + _)
      .map { case ((category: String, product: String), count: Long) => (category, (product, count)) }.groupByKey
      .mapValues(iter => iter.toList.sortWith(_._2 > _._2).take(3))

    val sep = ","
    result.collect().map(item => s"category: ${item._1} products: ${item._2.mkString(sep)}").toList
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Spark Pi")
    val sparkContext = new SparkContext(conf)

    val result: List[String] = getTop10MostFrequentlyPurchasedProductInEachCategory(sparkContext, "/user/cloudera/sampled/08/**")
    result.foreach { println }

    sparkContext.stop()
  }
}