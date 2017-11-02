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
import scala.collection.mutable.ListBuffer

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
      .load(geoSrc).select("geoname_id", "country_name").distinct()

    sqlContext.udf.register("addressToLong", (input: String) => {
      getNetAddress(input)
    })

    sqlContext.udf.register("checkAddresses", (address: String, network: String) => {
      checkAddresses(address, network)
    })

    csvIpAddress.join(csvCountryName, csvIpAddress.col("geoname_id") === csvCountryName.col("geoname_id"))
      .registerTempTable("ipCountry")
    val ipCountry = sqlContext.sql("select network, country_name, addressToLong(network) as networkLong from ipCountry").cache()

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

    val csvSaleDF = explodeDF.join(ipCountry, explodeDF.col("maskedaddress") === ipCountry.col("networkLong"))
    //  .filter("checkAddresses(ipaddress, network)")

    csvSaleDF.registerTempTable("salesDF");
    sqlContext.sql("select country_name , sum (price) as sumPrice from salesDF group by country_name where checkAddresses(ipaddress, network) order by sumPrice desc")
  }

  def getListOfSubNetAddresses(address: Long): List[Long] = {
    val result = ListBuffer[Long]()
    var mask: Long = ~0l
    var shiftsCount: Integer = 1
    var previouse: Long = -1
    while (shiftsCount < 25) {
      val changed: Long = address & mask
      if (changed != previouse) {
        result += changed
      }
      previouse = changed
      mask = mask << 1
      shiftsCount = shiftsCount + 1
    }

    return result.toList;
  }

  def getMaskedAdresses(address: String): List[Long] = {
    val result: List[Long] = List()
    if (address == null || address.isEmpty()) {
      return result;
    }
    val addrLong: Long = IpAddressToIntUDF.getLong(address)
    getListOfSubNetAddresses(addrLong)
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
      .setAppName("Spark")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)

    val df = selectTop10CountriesWithHighestMoney(sqlContext,
      "/user/cloudera/data/hive-external/esales_ext/**",
      "/user/cloudera/data/addresses.csv",
      "/user/cloudera/data/GeoLite2-City-Locations-en.csv")
    df.limit(100)
      .show(100)

    sparkContext.stop()
  }
}