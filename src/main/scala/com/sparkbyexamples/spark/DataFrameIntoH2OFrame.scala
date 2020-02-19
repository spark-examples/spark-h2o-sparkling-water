package com.sparkbyexamples.spark

import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SparkSession

object DataFrameIntoH2OFrame extends App {


  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate();

  val zipCodes = "src/main/resources/small_zipcode.csv"
  val zipCodesDF = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(zipCodes)


  zipCodesDF.printSchema()
  zipCodesDF.show(false)
  val h2oContext = H2OContext.getOrCreate(spark)
  val h2oFrame = h2oContext.asH2OFrame(zipCodesDF)

  println(h2oFrame._names.mkString(","))

  println(h2oFrame.names().mkString(","))

  println(h2oFrame.numRows())

  println(h2oFrame.numCols())

  h2oFrame.rename("zipcode","postcode")
  println(h2oFrame.names().mkString(","))

}
