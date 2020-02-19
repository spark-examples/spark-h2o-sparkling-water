package com.sparkbyexamples.spark

import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.sql.SparkSession

object H2OFrameToDataFrame extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate();

  val h2oContext = H2OContext.getOrCreate(spark)

  //Creating H20Frame
  import java.io.File
  val dataFile = "src/main/resources/small_zipcode.csv"
  val zipH2OFrame = new H2OFrame(new File(dataFile))

  //Convert H20Frame to Spark DataFrame
  val zipDF = h2oContext.asDataFrame(zipH2OFrame)

  zipDF.printSchema()
  zipDF.show(false)

}
