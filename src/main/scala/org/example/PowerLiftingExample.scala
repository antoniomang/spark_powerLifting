package org.example


import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object PowerLiftingExample extends App {

  //Start a new Spark Session
  val spark = SparkSession.builder()
    .appName("Prova per Powerlifting Project")
    .config("spark.master", "local")
    .getOrCreate()

  //Define Schemas for both csv
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

  //Read both DataFrames, print schemas, show first 20 rows
  val powerDF = spark.read
    .schema(powerLiftingSchema)
    .option("header", "true")
    .csv("src/main/resources/Data/meets.csv")

  powerDF.printSchema()
  powerDF.show()

  val openPowerLiftingDF = spark.read
    .schema(openPowerLiftingSchema)
    .option("header", "true")
    .csv("src/main/resources/Data/openpowerlifting.csv")

  openPowerLiftingDF.printSchema()
  openPowerLiftingDF.show()


  //Count all rows in both Dataframes
  val countPowerDF = powerDF.select(count("*"))

  countPowerDF.show

  val countOpenPowerLiftingDF = openPowerLiftingDF.select(count("*"))

  countOpenPowerLiftingDF.show



  /*
  ************************************
  Data Exploring
  ************************************
  */

  //Latest Dates = 2018-1974
  val powerOrderDateDF = powerDF.select(col("*"))
    .orderBy(col("Date").desc)

  powerOrderDateDF.show()

  //Max Date = 2018-01-28
  powerDF.select(max(col("Date"))).show()

  //Min Date = 1974-03-02
  powerDF.select(min(col("Date"))).show()

  //Count Distinct on MeetCountry = 45
  powerDF.select(countDistinct("MeetCountry")).show()

  //Count Distinct on MeetState = 80
  powerDF.select(countDistinct("MeetState")).show()

  //Equipment NULL = 0
  openPowerLiftingDF.select(count("*")).show()
  openPowerLiftingDF.where(col("Equipment").isNotNull).select(count("Equipment")).show()

  //Count NULL Dates = 0
  val powerNullDateDF = powerDF
    .where(col("Date").isNotNull)
    .select(count("Date")).show()



  /*
   ************************************
   Data Processing
   ************************************
   */

  //To deal with NULL Values on BestSquat, BestBench and BestDeadlift columns,
  //we will take for granted that athletes are able to lift at least their Body Weight
  val openPowerLiftingSolvingNullDF = openPowerLiftingDF
    .withColumn("BestSquatKg", when(col("BestSquatKg").isNull,
      round(col("BodyweightKg"))).otherwise(col("BestSquatKg")))
    .withColumn("BestBenchKg", when(col("BestBenchKg").isNull,
      round(col("BodyweightKg"))).otherwise(col("BestBenchKg")))
    .withColumn("BestDeadliftKg", when(col("BestDeadliftKg").isNull,
      round(col("BodyweightKg"))).otherwise(col("BestDeadliftKg")))
    .drop("Squat4Kg", "Bench4Kg", "Deadlift4Kg")

  //Populate TotalKg where it is NULL
  val openPowerLiftingTotalSolvingNullNewDF = openPowerLiftingSolvingNullDF
    .withColumn("TotalNewKg", when(col("TotalKg").isNull,
      col("BestSquatKg") + col("BestBenchKg") + col("BestDeadliftKg")).otherwise(col("TotalKg")))

  //We will drop all the rows where TotalNewKg is NULL (BodyweightKg|WeightClassKg|BestSquatKg|BestBenchKg are NULLS)
  val openPowerLiftingTotalNoNullDF = openPowerLiftingTotalSolvingNullNewDF.na.drop(Seq("TotalNewKg"))


  /*
   NULL values on the Squat4Kg, Bench4Kg, Deadlift4Kg will be evaluated as follows:
   -Squat4Kg -> Coefficient of Squat = BestSquatKg/BodyweightKg
   Coefficient Of Squat = The more the COS is higher the more the strength per Kg of BodyWeight is higher
   Example: 142.88 BestSquatKg/58.51 BodyweightKg = 2.44 COS ( Each kg of bodyweight can pull 2.44 kgs of squat)
   The same will be done on Coefficient of Bench and Deadlift
    */
  val openPowerLiftingNewCoefficientsDF = openPowerLiftingTotalNoNullDF
    .withColumn("Coefficient of Squat", col("BestSquatKg") / col("BodyweightKg"))
    .withColumn("Coefficient of Bench", col("BestBenchKg") / col("BodyweightKg"))
    .withColumn("Coefficient of Deadlift", col("BestDeadliftKg") / col("BodyweightKg"))
    .select(col("*"))
    .drop("Squat4Kg", "Bench4Kg", "Deadlift4Kg")

  //The old Wilks Coefficient will be replaced by the sum of the 3 Coefficients we previously calculated
  val openPowerLiftingFinalDF = openPowerLiftingNewCoefficientsDF
    .withColumn("Total Strenght Coefficient", col("BestSquatKg") + col ("BestBenchKg") + col("BestDeadliftKg"))
    .drop(col("Wilks"), col("Coefficient of Squat"), col ("Coefficient of Bench"),  col("Coefficient of Deadlift"),
      col("TotalKg"))

  //Join both DataFrames and select only competitions from 2015-01-01, order by ascending Date
  val joinBetweenDF = openPowerLiftingFinalDF
    .join(powerDF, openPowerLiftingFinalDF.col("MeetID") === powerDF.col("MeetID"), "inner")
    .where(powerDF.col("Date") >= "2015-01-01" )
    .orderBy(col("Date").asc)

  //Group by the following fields and obtain a Sum of the TotalNewKg to see which federation has the strongest competitors

  val joinGroupFederationDF = joinBetweenDF
    .groupBy("Name","BodyweightKg","Place","Total Strenght Coefficient", "Federation")
    .agg(sum(col("TotalNewKg")).as("SumTotal"))
    .orderBy(col("SumTotal").desc)
    .show()


}
