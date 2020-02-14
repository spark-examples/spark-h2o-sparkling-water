package com.sparkbyexamples.spark

import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SparkSession

object H20FrameFromDataFrame extends App {


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
  val h20Frame = h2oContext.asH2OFrame(zipCodesDF)

  println(h20Frame._names.mkString(","))

  println(h20Frame.names().mkString(","))

  println(h20Frame.numRows())

  println(h20Frame.numCols())

  h20Frame.rename("zipcode","postcode")
  println(h20Frame.names().mkString(","))

}
