package com.sample
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col

import org.apache.spark.sql.types.{TimestampType, StructType, StructField, StringType, DoubleType}
  
/*
 * DEPRECATED!!!! Wrong requirements assumption!
 */

object Top10CountriesDFJob {
  
  def selectTop10CountriesWithHighestMoney (sc : SparkContext, src: String, adrSrc:String, geoSrc : String) = {
    
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val csvIpAddress = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      //      .schema(addressSchema)
      .option("inferSchema", "true")
      .load(adrSrc).select("network", "geoname_id")
//    csvIpAddress.show()
    
    val csvCountryName = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(geoSrc).select("geoname_id", "country_name")
      
    csvIpAddress.join(csvCountryName, csvIpAddress.col("geoname_id") === csvCountryName.col("geoname_id"))
      .registerTempTable("ipCountry")
    val ipCountry = sqlContext.sql("select network, country_name, addressToLong(network) as networkLong from ipCountry")

    val csv = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .load(src)
    val csvDf = csv.select(csv.col("C2").alias("price"), csv.col("C4").alias("ipaddress"))
      
    val csvSaleDF = csvDf.join(ipCountry, csvDf.col("ipaddress")===ipCountry.col("f1_ipAddress"))
    csvSaleDF.registerTempTable("salesDF");
    sqlContext.sql("select ff6_countryName , sum (price) as sumPrice from salesDF group by ff6_countryName order by sumPrice desc")
    
  }
  
  def main(args: Array[String]) = {
    val conf = new SparkConf()
             .setAppName("Spark")
    val sparkContext  = new SparkContext(conf)
    
    val result = selectTop10CountriesWithHighestMoney(sparkContext, "/user/cloudera/sampled/08/**", "/user/cloudera/data/addresses.csv",
        "/user/cloudera/data/GeoLite2-City-Locations-en.csv")
    result.limit(10)
      .show(10)

    sparkContext.stop()  
  }
}