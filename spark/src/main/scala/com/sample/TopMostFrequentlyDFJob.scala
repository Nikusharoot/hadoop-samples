package com.sample
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{ rowNumber, max, broadcast }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.types.{ TimestampType, StructType, StructField, StringType, DoubleType }

object TopMostFrequentlyDFJob {

  def getTop10MostFrequentlyPurchasedCategories(sqlContext: SQLContext, src: String) = {

    val csvDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .load(src).select(col("C1").alias("product"), col("C2").alias("price"), col("C3").alias("category"))

    csvDf.registerTempTable("sales")
    val sales = sqlContext.sql("select category, count(*) as amount from sales group by category order by amount desc").limit(10)
    sales.printSchema()
    sales.show()
  }

  def getTop10MostFrequentlyPurchasedProductInEachCategory(sqlContext: SQLContext, src: String): DataFrame = {

    val csvDf = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .load(src).select(col("C1").alias("product"), col("C2").alias("price"), col("C3").alias("category"))

    csvDf.registerTempTable("sales")
    val sales = sqlContext.sql("select category, product, count(*) as amount from sales group by category, product")
    val w = Window.partitionBy(col("category")).orderBy(col("amount").desc)

    sales.withColumn("rn", rowNumber.over(w)).where(col("rn") <= 10).drop("rn")
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Spark Pi")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)
    val dfTop = getTop10MostFrequentlyPurchasedProductInEachCategory(sqlContext, "/user/cloudera/sampled/08/**")
    dfTop.show
    sparkContext.stop()
  }
}