package com.sparkbyexamples.spark

import org.apache.spark.h2o._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparklingWaterExample extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate();

  val h2oContext = H2OContext.getOrCreate(spark)

  import h2oContext._
  import spark.implicits._
  val weatherDataFile = "src/main/resources/Chicago_Ohare_International_Airport.csv"
  val weatherTable = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(weatherDataFile)
    .withColumn("Date", to_date('Date, "MM/dd/yyyy"))
    .withColumn("Year", year('Date))
    .withColumn("Month", month('Date))
    .withColumn("DayofMonth", dayofmonth('Date))

  weatherTable.printSchema()
  weatherTable.show(false)

  import java.io.File
  val dataFile = "src/main/resources/allyears2k_headers.zip"
  val airlinesH2OFrame = new H2OFrame(new File(dataFile))
  val airlinesTable = h2oContext.asDataFrame(airlinesH2OFrame)
  val flightsToORD = airlinesTable.filter('Dest === "ORD")

  val joinedDf = flightsToORD.join(weatherTable, Seq("Year", "Month", "DayofMonth"))

  import water.support.H2OFrameSupport._
  val joinedHf = columnsToCategorical(h2oContext.asH2OFrame(joinedDf), Array("Year", "Month", "DayofMonth"))

  import _root_.hex.deeplearning.DeepLearning
  import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters
  import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
  val dlParams = new DeepLearningParameters()
  dlParams._train = joinedHf.key
  dlParams._response_column = "ArrDelay"
  dlParams._epochs = 5
  dlParams._activation = Activation.RectifierWithDropout
  dlParams._hidden = Array[Int](100, 100)

  // Create a job
  val dl = new DeepLearning(dlParams)
  val dlModel = dl.trainModel.get

  val predictionsHf = dlModel.score(joinedHf)
  val predictionsDf = h2oContext.asDataFrame(predictionsHf)

  predictionsDf.show(false)
  //import org.apache.spark.examples.h2o.AirlinesWithWeatherDemo2.residualPlotRCode
  //residualPlotRCode(predictionsHf, "predict", joinedDf, "ArrDelay", hc)
}
