package com.sample

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

import com.sample.udf.AddressInNetCheckerUDF
import com.sample.udf.BitsMaskMoveUDF
import com.sample.udf.IpAddressToIntUDF
import com.sample.udf.NetMaskAddressLowIntUDF
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Top10CountriesByNetMaskDFJob {

  def selectTop10CountriesWithHighestMoney(sqlContext: SQLContext, src: String, adrSrc: String, geoSrc: String) = {

    val csvIpAddress = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(adrSrc).select("network", "geoname_id")

    val csvCountryName = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(geoSrc).select("geoname_id", "country_name")

    sqlContext.udf.register("addressToLong", (input: String) => {
      getNetAddress(input)
    })

    sqlContext.udf.register("checkAddresses", (address: String, network: String) => {
      checkAddresses(address, network)
    })

    csvIpAddress.join(csvCountryName, csvIpAddress.col("geoname_id") === csvCountryName.col("geoname_id"))
      .registerTempTable("ipCountry")
    val ipCountry = sqlContext.sql("select network, country_name, addressToLong(network) as networkLong from ipCountry").distinct()
        ipCountry.show()

    val csv = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .load(src)
    val csvDf = csv.select(csv.col("C2").alias("price"), csv.col("C4").alias("ipaddress"))

    val explodeDF2 = csvDf.explode(col("price"), col("ipaddress")) {
      case row: Row =>
        val address = row(1).asInstanceOf[String]
        val addresses = getMaskedAdresses(address)
        addresses.map(maskedaddress => (address, maskedaddress))
    }
    val explodeDF = explodeDF2.toDF("price", "ipaddress", "ipaddress2", "maskedaddress")
        explodeDF.show(100)

    val csvSaleDF = explodeDF.join(ipCountry, explodeDF.col("maskedaddress") === ipCountry.col("networkLong"))
      .filter("checkAddresses(ipaddress, network)")
        csvSaleDF.show()

    csvSaleDF.registerTempTable("salesDF");
    sqlContext.sql("select country_name , sum (price) as sumPrice from salesDF group by country_name order by sumPrice desc")
  }

  def getMaskedAdresses(address: String): Array[Long] = {
    val result: Array[Long] = Array()
    if (address == null || address.isEmpty()) {
      return result;
    }
    val addrLong: Long = IpAddressToIntUDF.getLong(address)
    BitsMaskMoveUDF
      .getListOfSubNetAddresses(addrLong).asScala.map { lw =>
        lw.get()
      }.toArray
  }

  def getNetAddress(input: String): Long = {
    Option(input) match {
      case Some(s) => {
        val str: String = s.toString();
        val pos: Integer = str.indexOf("/");
        val addresskStr: String = NetMaskAddressLowIntUDF.validateAddress(str, pos);
        IpAddressToIntUDF.getLong(addresskStr);
      }
      case _ => -1
    }
  }

  def checkAddresses(address: String, network: String): Boolean = {
    (Option(address), Option(network)) match {
      case (Some(a), Some(n)) if (!a.isEmpty && !n.isEmpty) => {
        AddressInNetCheckerUDF.checkAdresses(a, n);
      }
      case _ => false
    }
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Spark Pi")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)

    val df = selectTop10CountriesWithHighestMoney(sqlContext,
      "/user/cloudera/sampled/08/**",
      "/user/cloudera/data/addresses.csv",
      "/user/cloudera/data/GeoLite2-City-Locations-en.csv")
    df.limit(10)
      .show(10)

    sparkContext.stop()
  }
}