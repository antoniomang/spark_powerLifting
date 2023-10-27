package org.example


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types._

import java.sql.Struct

object PowerLiftingExample extends App {

  val spark = SparkSession.builder()
    .appName("Prova per Powerlifting Project")
    .config("spark.master", "local")
    .getOrCreate()


  val powerLiftingSchema = StructType(Array(
    StructField("MeetID", IntegerType),
    StructField("MeetPath", StringType),
    StructField("Federation", StringType),
    StructField("Date", DateType),
    StructField("MeetCountry", StringType),
    StructField("MeetState", StringType),
    StructField("MeetTown", StringType),
    StructField("MeetName", StringType)
  )
  )

  val openPowerLiftingSchema = StructType(Array(
    StructField("MeetID", IntegerType),
    StructField("Name", StringType),
    StructField("Sex", StringType),
    StructField("Equipment", StringType),
    StructField("Age", LongType),
    StructField("Division", StringType),
    StructField("BodyweightKg", FloatType),
    StructField("WeightClassKg", DoubleType),
    StructField("Squat4Kg", DoubleType),
    StructField("BestSquatKg", DoubleType),
    StructField("Bench4Kg", DoubleType),
    StructField("BestBenchKg", DoubleType),
    StructField("Deadlift4Kg", DoubleType),
    StructField("BestDeadliftKg", DoubleType),
    StructField("TotalKg", DoubleType),
    StructField("Place", IntegerType),
    StructField("Wilks", DoubleType),
  )
  )




  val powerDF = spark.read
    .schema(powerLiftingSchema)
    .option("header", "true")
    .csv("src/main/resources/Data/meets.csv")

  powerDF.printSchema()


  val openPowerLiftingDF = spark.read
    .schema(openPowerLiftingSchema)
    .option("header", "true")
    .csv("src/main/resources/Data/openpowerlifting.csv")



  val powerNewDF = powerDF.show()

  val openPowerLiftingNewDF = openPowerLiftingDF.show()

}
