package com.sparkbyexamples.spark


import org.apache.spark.h2o.{H2O, H2OContext, H2OFrame}
import org.apache.spark.sql.SparkSession

object CreateH2OFrame extends App{
  import java.io.File

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate();

  val h2oContext = H2OContext.getOrCreate(spark)

  val dataFile = "src/main/resources/small_zipcode.csv"
  val zipCodeFrame = new H2OFrame(new File(dataFile))
  println(zipCodeFrame.names().mkString(","))

  val parquetDataFile = "src/main/resources/zipcodes.parquet"
  val zipCodeParquetFrame = new H2OFrame(new File(parquetDataFile))
  println(zipCodeParquetFrame.names().mkString(","))
}
