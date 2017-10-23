package com.sample
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{ rowNumber, max, broadcast }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import com.sample.udf.AddressInNetCheckerUDF

import org.apache.spark.sql.types.{ TimestampType, StructType, StructField, StringType, DoubleType }

object Top10CountriesNetmaskRDDJob {

  def selectTop10CountriesWithHighestMoney(sc: SparkContext, src: String, adrSrc: String, geoSrc: String) = {

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val csvIpAddress = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(adrSrc).rdd.map(item => (item.getString(1), item.getString(0)))

    val csvCountryName = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(geoSrc).rdd.map(item => (item.getString(0), item.getString(5)))

    val joinedCountries = csvIpAddress.join(csvCountryName)

    val ipCountry = joinedCountries.map(item =>
      (Top10CountriesByNetMaskDFJob.getNetAddress(item._2._1), (item._2._1, item._2._2)))
      .reduceByKey { case (a, b) => a }

    val csv = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .load(src)

    val csvSale = csv.rdd.map(item => (item.getString(4), item.getDouble(2)))

    val convert: ((String, Double)) => List[(Long, (String, Double))] =
      tuple => Top10CountriesByNetMaskDFJob.getMaskedAdresses(tuple._1).map(longAddress =>
        (longAddress, (tuple._1, tuple._2))).toList

    val csvSaleRDD = csvSale.flatMap(convert)

    val joinedSales = csvSaleRDD.join(ipCountry)

    val filteredSales = joinedSales.filter(f => Top10CountriesByNetMaskDFJob.checkAddresses(f._2._1._1, f._2._2._1))
      .map(f => (f._2._2._2, f._2._1._2))

    filteredSales.reduceByKey(_ + _).sortBy(_._2, false)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Spark Pi")
    val sparkContext = new SparkContext(conf)

    selectTop10CountriesWithHighestMoney(sparkContext,
      "/user/cloudera/sampled/08/**",
      "/user/cloudera/data/addresses.csv",
      "/user/cloudera/data/GeoLite2-City-Locations-en.csv")

    sparkContext.stop()
  }
}