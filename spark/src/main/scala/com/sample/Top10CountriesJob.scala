package com.sample
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

import org.apache.spark.sql.types.{TimestampType, StructType, StructField, StringType, DoubleType}
 /*
 * DEPRECATED!!!! Wrong requirements assumption!
 */
 
object Top10CountriesJob {

  def selectTop10CountriesWithHighestMoney (sc : SparkContext) = {
    
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val addressSchema = StructType(Array(
    StructField("f1", StringType, true),
    StructField("f2", StringType, true),
    StructField("f3", StringType, true),
    StructField("f4", StringType, true),
    StructField("f5", StringType, true),
    StructField("f6", StringType, true),
    StructField("f7", StringType, true),
    StructField("f8", StringType, true),
    StructField("f9", StringType, true),
    StructField("f_last", StringType, true)))

    
    val csvIpAddress = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(addressSchema)
      .load("/user/cloudera/data/addresses.csv").rdd.map(item => (item.getString(1), item.getString(0)))
     
    val countrySchema = StructType(Array(
    StructField("f1", StringType, true),
    StructField("f2", StringType, true),
    StructField("f3", StringType, true),
    StructField("f4", StringType, true),
    StructField("f5", StringType, true),
    StructField("f6", StringType, true),
    StructField("f7", StringType, true),
    StructField("f8", StringType, true),
    StructField("f9", StringType, true),
    StructField("f10", StringType, true),
    StructField("f11", StringType, true),
    StructField("f12", StringType, true),
    StructField("f_last", StringType, true)))
      
    val csvCountryName = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(countrySchema)
      .load("/user/cloudera/data/GeoLite2-City-Locations-en.csv").rdd.map(item => (item.getString(0), item.getString(5)))
      
    val IpCountry = csvIpAddress.join(csvCountryName).map(item=>(item._2._1, item._2._2))
      
    val customSchema = StructType(Array(
    StructField("datetime", TimestampType, true),
    StructField("product", StringType, true),
    StructField("price", DoubleType, true),
    StructField("category", StringType, true),
    StructField("ipaddress", StringType, true)))
    
    val csvDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .schema(customSchema)
      .load("/user/cloudera/sampled/08/**")
      
    val csvSaleRDD = csvDf.rdd.map(item=>(item.getString(4), item.getDouble(2)))  
    
    csvSaleRDD.join(IpCountry).map(item => (item._2._2, item._2._1)).reduceByKey(_ + _).sortBy(_._2, false).take(10).foreach(println)
  }
  
  def main(args: Array[String]) = {
    val conf = new SparkConf()
             .setAppName("Spark")
    val sparkContext  = new SparkContext(conf)
    
    selectTop10CountriesWithHighestMoney(sparkContext)
    
    sparkContext.stop()  
  }
}