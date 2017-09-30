package com.sample
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{rowNumber, max, broadcast}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

import org.apache.spark.sql.types.{TimestampType, StructType, StructField, StringType, DoubleType}
import org.apache.spark.sql.DataFrame
  
object SimpleCountJob {
 
  def selectTop10CountriesWithHighestMoney (sqlContext : SQLContext, src1 : String, src2 : String, src3 : String):DataFrame = {
   
    val addressSchema = StructType(Array(
    StructField("f1_ipAddress", StringType, true),
    StructField("f2_id", StringType, true),
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
      .load(src1)
      //.rdd.map(item => (item.getString(1), item.getString(0)))
     
    val countrySchema = StructType(Array(
    StructField("ff1_id", StringType, true),
    StructField("ff2", StringType, true),
    StructField("ff3", StringType, true),
    StructField("ff4", StringType, true),  
    StructField("ff5", StringType, true),
    StructField("ff6_countryName", StringType, true),
    StructField("ff7", StringType, true),
    StructField("ff8", StringType, true),
    StructField("ff9", StringType, true),
    StructField("f10", StringType, true),
    StructField("ff11", StringType, true),
    StructField("ff12", StringType, true),
    StructField("ff_last", StringType, true))) 
      
    val csvCountryName = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(countrySchema)
      .load(src2)
      //.rdd.map(item => (item.getString(0), item.getString(5)))
      
    val ipCountry = csvIpAddress.join(csvCountryName, csvIpAddress.col("f2_id")===csvCountryName.col("ff1_id"))
      .drop("ff2")
      .drop("ff3")
      .drop("ff4")
      .drop("ff5")
      .drop("ff7")
      .drop("ff8")
      .drop("ff9")
      .drop("ff10")
      .drop("ff11")
      .drop("ff12")
      .drop("ff_last")
      .drop("f3")
      .drop("f4")
      .drop("f5")
      .drop("f6")
      .drop("f7")
      .drop("f8")
      .drop("f9")
      .drop("f_last")
      
    val customSchema = StructType(Array(
    StructField("datetime", TimestampType, true),
    StructField("product", StringType, true),
    StructField("price", DoubleType, true),
    StructField("category", StringType, true),
    StructField("ipaddress", StringType, true)))
    
    val csvDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .schema(customSchema)
      .load(src3)
      .drop("datetime")
      .drop("product")
      .drop("category")
      
    val csvSaleDF = csvDf.join(ipCountry, csvDf.col("ipaddress")===ipCountry.col("f1_ipAddress"))
    //.rdd.map(item=>(item.getString(4), item.getDouble(2)))  
    csvSaleDF.registerTempTable("salesDF");
    sqlContext.sql("select ff6_countryName , sum (price) as sumPrice from salesDF group by ff6_countryName order by sumPrice desc")
      .limit(10)
    csvSaleDF.show(10)
    csvSaleDF
  }

  def extracted(csvSaleDF : DataFrame, table: String) = {
      csvSaleDF.write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/")
        .option("dbtable", table)
        .option("user", "root")
        .option("password", "cloudera")
        .save()
    }

  
  def main(args: Array[String]) = {
    val conf = new SparkConf()
             .setAppName("Spark Pi")
    val sparkContext  = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)
    val df = selectTop10CountriesWithHighestMoney(sqlContext, 
        "/user/cloudera/data/addresses.csv", 
        "/user/cloudera/data/GeoLite2-City-Locations-en.csv",
        "/user/cloudera/sampled/08/**")
    extracted(df, "sales8")
    sparkContext.stop()  
  }
}