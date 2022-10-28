package com.project.application

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object App {

  /**
   * Read the sensors measurements csv files in data directory
   * calculate the stats and print it to the console
   * @param args
   */
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    // Input path files
    val inputFilesPath = "data"

    // Initialize Spark
    val spark = SparkSession.builder.master("local[*]").appName("Sensor Stats").getOrCreate()
    val sensorDF = spark.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(inputFilesPath)

    // Get number of process files
    val numOfProcessedFiles = sensorDF.rdd.getNumPartitions

    // Get number of measurements
    val measurements = sensorDF.select("humidity").count

    // Get number of NaN measurements
    val failedMeasurement = sensorDF.filter("humidity == 'NaN'").count

    println(f"Num of processed files: $numOfProcessedFiles")
    println(f"Num of processed measurements: $measurements")
    println(f"Num of failed measurements: $failedMeasurement")

    // Get min, avg, max sensor reading
    val statsDf = sensorDF.groupBy("sensor-id").agg(
      min("humidity") as ("min"),
      avg("humidity") as ("avg"),
      max("humidity") as ("max"),
    )
      .sort(asc("avg"))

    // Print the results
    statsDf.show(false)

    // Stop the program
    spark.stop()
  }
}
