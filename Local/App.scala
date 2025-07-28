import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Random
import scala.reflect.ClassTag

object Main extends App {

  private val spark = SparkSession.builder
    .master("local[*]")
    .appName("NYC Taxi Analyzer")
    .getOrCreate()

  //data for 2021-2023 period. (9 gb)
  private val taxiRawDataPath = "hdfs://localhost:9000/taxi_data/yellow_taxi_combined_2021_2023.csv"

  //defining the schema to properly access the data in the csv file.
  val schema = StructType(Array(
    StructField("VendorID", StringType, nullable = true),
    StructField("tpep_pickup_datetime", StringType, nullable = true),
    StructField("tpep_dropoff_datetime", StringType, nullable = true),
    StructField("passenger_count", DoubleType, nullable = true),
    StructField("trip_distance", DoubleType, nullable = true),
    StructField("RatecodeID", StringType, nullable = true),
    StructField("store_and_fwd_flag", StringType, nullable = true),
    StructField("PULocationID", StringType, nullable = true),
    StructField("DOLocationID", StringType, nullable = true),
    StructField("payment_type", StringType, nullable = true),
    StructField("fare_amount", DoubleType, nullable = true),
    StructField("extra", DoubleType, nullable = true),
    StructField("mta_tax", DoubleType, nullable = true),
    StructField("tip_amount", DoubleType, nullable = true),
    StructField("tolls_amount", DoubleType, nullable = true),
    StructField("improvement_surcharge", DoubleType, nullable = true),
    StructField("total_amount", DoubleType, nullable = true),
    StructField("congestion_surcharge", DoubleType, nullable = true),
    StructField("airport_fee", DoubleType, nullable = true)
  ))

//  private val taxiDF = spark.read
//    .option("header", "true")
//    .schema(schema)
//    .csv(taxiRawDataPath)
//    .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
//    .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
//    .withColumn("hour", hour(col("pickup_datetime")))
//    .filter(col("passenger_count") > 0)  //filtering some invalid cases.
  //    .filter(col("tip_amount") >= 0)

  private val taxiDF = spark.read
    .option("header", "true")
    .schema(schema)
    .csv(taxiRawDataPath)
    .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    .withColumn("hour", hour(col("pickup_datetime")))
    .filter(col("passenger_count") > 0)
    .filter(
      col("pickup_datetime").between("2021-01-01", "2023-12-31")
    )

  println(s"Total rows: ${taxiDF.count()}")

  //taxiDF.limit(10).show(truncate = false)

  //we're going to use Algorithm R to get our sample.
  private def reservoirSample[T: ClassTag](input: Iterator[T], k: Int): Array[T] = {
    val reservoir = new Array[T](k)
    val random = new Random()

    for ((elem, i) <- input.zipWithIndex) {
      if (i < k) {
        reservoir(i) = elem
      } else {
        val j = random.nextInt(i + 1)
        if (j < k) {
          reservoir(j) = elem
        }
      }
    }

    reservoir
  }

  //getting the sample from the algorithm.
  //100_000  initial value, perhaps it can be changed later.
  //The limit on iter was put due to Q3 being too heavy.
  private val sampleSize = 100_000
  private val iter = taxiDF/*.limit(10000000)*/.toLocalIterator().asScala //before we call our samplingMethod we gather all data on the driver node
  private val sampledRows = reservoirSample(iter, sampleSize) //we then do the sampling
  private val sampleRDD = spark.sparkContext.parallelize(sampledRows.toList) //then redistribute the data again.
  private val sampleDF = spark.createDataFrame(sampleRDD, taxiDF.schema) //our sample dataframe is ready


  println(s"Sample size: ${sampleDF.count()} rows")

//  //Query1: Peak Hour Tipping Behavior – Compute average tips per passenger by hour.
//
//  //No sample
//  val start = System.nanoTime()
//
//  private val query1DF = taxiDF
//    .filter(col("passenger_count") > 0)
//    .filter(col("tip_amount") >= 0)
//    .withColumn("tip_per_passenger", col("tip_amount") / col("passenger_count"))
//    .groupBy("hour")
//    .agg(
//      round(avg("tip_per_passenger"), 2).alias("avg_tip_per_passenger"),
//      count("*").alias("trip_count")
//    )
//    .orderBy("hour")
//
//  val end = System.nanoTime()
//
//  private val durationSeconds = (end - start) / 1e9d
//  // Show the results
//  query1DF.show()
//  println(f"Query 1 executed in $durationSeconds%.2f seconds")
//
//  //With sample
//  private val startSample = System.nanoTime()
//  private val query1SampleDF = sampleDF
//    .filter(col("tip_amount") >= 0)
//    .withColumn("tip_per_passenger", col("tip_amount") / col("passenger_count"))
//    .groupBy("hour")
//    .agg(
//      round(avg("tip_per_passenger"), 2).alias("avg_tip_per_passenger"),
//      count("*").alias("trip_count")
//    )
//    .orderBy("hour")
//  private val endSample = System.nanoTime()
//  private val timeSample = (endSample - startSample) / 1e9
//
//  query1SampleDF.show()
//  println(f"Query 1 (Sample) executed in $timeSample%.2f seconds")
//
//  //END OF QUERY 1

  //Query2: Top Revenue Pickup Zones – Identify zones with the highest average fare.
  //No sample
//  private val start2 = System.nanoTime()
//  private val query2DF = taxiDF
//    .filter(col("fare_amount").isNotNull && col("fare_amount") > 0)
//    .groupBy("PULocationID")
//    .agg(
//      round(avg("fare_amount"), 2).alias("avg_fare_amount"),
//      count("*").alias("trip_count")
//    )
//    .orderBy(desc("avg_fare_amount"))
//    .limit(10)
//  private val end2 = System.nanoTime()
//  private val duration2 = (end2 - start2) / 1e9d
//  query2DF.show(false)
//  println(f"Query 2 executed in $duration2%.2f seconds")
//
//  //With sample
//  private val start2Sample = System.nanoTime()
//  private val query2SampleDF = sampleDF
//    .filter(col("fare_amount").isNotNull && col("fare_amount") > 0)
//    .groupBy("PULocationID")
//    .agg(
//      round(avg("fare_amount"), 2).alias("avg_fare_amount"),
//      count("*").alias("trip_count")
//    )
//    .orderBy(desc("avg_fare_amount"))
//    .limit(10)
//  private val end2Sample = System.nanoTime()
//  private val duration2Sample = (end2Sample - start2Sample) / 1e9d
//  query2SampleDF.show(false)
//  println(f"Query 2 (Sample) executed in $duration2Sample%.2f seconds")



  //Query3: Multidimensional Skyline – Find trips not dominated in tip, fare, and distance.
  //No sample
//  private val startSkyline = System.nanoTime()
//
//  private val taxiStats = taxiDF
//    .filter(
//      col("tip_amount").isNotNull &&
//        col("fare_amount").isNotNull &&
//        col("trip_distance").isNotNull &&
//        col("tip_amount") >= 0 &&
//        col("fare_amount") >= 0 &&
//        col("trip_distance") >= 0
//    )
//    //.limit(10000000)
//    .select("tip_amount", "fare_amount", "trip_distance")
//
//  private val statsA = taxiStats.alias("a")
//  private val statsB = taxiStats.alias("b")
//
//  private val dominationCondition =
//    (col("b.tip_amount") >= col("a.tip_amount")) &&
//      (col("b.fare_amount") >= col("a.fare_amount")) &&
//      (col("b.trip_distance") >= col("a.trip_distance")) &&
//      (
//        col("b.tip_amount") > col("a.tip_amount") ||
//          col("b.fare_amount") > col("a.fare_amount") ||
//          col("b.trip_distance") > col("a.trip_distance")
//        )
//
//  private val skylineFullDF = statsA.join(statsB, dominationCondition, "left_anti")
//
//  private val endSkyline = System.nanoTime()
//  private val durationSkyline = (endSkyline - startSkyline) / 1e9d
//
//  skylineFullDF.show(truncate = false)
//  println(f"Query 3 (Skyline - Full Data) executed in $durationSkyline%.2f seconds")
//  println(s"Skyline size (full data): ${skylineFullDF.count()} rows")
//
//
//  //With sample
//  private val startSkylineSample = System.nanoTime()
//
//  private val sampleStats = sampleDF
//    .filter(
//      col("tip_amount").isNotNull &&
//        col("fare_amount").isNotNull &&
//        col("trip_distance").isNotNull &&
//        col("tip_amount") >= 0 &&
//        col("fare_amount") >= 0 &&
//        col("trip_distance") >= 0
//    )
//    .select("tip_amount", "fare_amount", "trip_distance")
//
//  private val sampleA = sampleStats.alias("a")
//  private val sampleB = sampleStats.alias("b")
//
//  private val dominationSampleCondition =
//    (col("b.tip_amount") >= col("a.tip_amount")) &&
//      (col("b.fare_amount") >= col("a.fare_amount")) &&
//      (col("b.trip_distance") >= col("a.trip_distance")) &&
//      (
//        col("b.tip_amount") > col("a.tip_amount") ||
//          col("b.fare_amount") > col("a.fare_amount") ||
//          col("b.trip_distance") > col("a.trip_distance")
//        )
//
//  private val skylineSampleDF = sampleA.join(sampleB, dominationSampleCondition, "left_anti")
//
//  private val endSkylineSample = System.nanoTime()
//  private val durationSkylineSample = (endSkylineSample - startSkylineSample) / 1e9d
//
//  skylineSampleDF.show(truncate = false)
//  println(f"Query 3 (Skyline - Sample) executed in $durationSkylineSample%.2f seconds")
//  println(s"Skyline size (sample): ${skylineSampleDF.count()} rows")

  //Query4: Congestion Surcharge Trends – Analyze total congestion fees per month.
  //No sample
//  private val startQ4Full = System.nanoTime()
//
//  private val query4FullDF = taxiDF
//    .withColumn("year", year(col("pickup_datetime")))
//    .withColumn("month", month(col("pickup_datetime")))
//    .groupBy("year", "month")
//    .agg(
//      round(sum("congestion_surcharge"), 2).alias("total_congestion_surcharge"),
//      count("*").alias("trip_count")
//    )
//    .orderBy("year", "month")
//
//  private val endQ4Full = System.nanoTime()
//  private val durationFull = (endQ4Full - startQ4Full) / 1e9
//
//  query4FullDF.show(50, truncate = false)
//  println(f"Query 4 (Full Data) executed in $durationFull%.2f seconds")
//
//  //With sample
//  private val startQ4Sample = System.nanoTime()
//
//  private val query4SampleDF = sampleDF
//    .withColumn("year", year(col("pickup_datetime")))
//    .withColumn("month", month(col("pickup_datetime")))
//    .groupBy("year", "month")
//    .agg(
//      round(sum("congestion_surcharge"), 2).alias("total_congestion_surcharge"),
//      count("*").alias("trip_count")
//    )
//    .orderBy("year", "month")
//
//  private val endQ4Sample = System.nanoTime()
//  private val durationSample = (endQ4Sample - startQ4Sample) / 1e9
//
//  query4SampleDF.show(50, truncate = false)
//  println(f"Query 4 (Sample Data) executed in $durationSample%.2f seconds")

  //

  // Query5: Display the top 50 weekday morning dropoff locations
  // No sample version
  private val startQ5 = System.nanoTime()

  private val query5FullDF = taxiDF
    .withColumn("hour", hour(col("dropoff_datetime")))
    .withColumn("day_of_week", expr("extract(DOW from dropoff_datetime) + 1")) // 1=Monday, ..., 7=Sunday
    .filter(col("hour").between(6, 11))
    .filter(col("day_of_week").between(1, 5)) // Weekdays only
    .filter(col("DOLocationID").isNotNull)
    .groupBy("DOLocationID", "hour")
    .agg(count("*").alias("dropoff_count"))
    .orderBy(desc("dropoff_count"))
    .limit(50)

  private val endQ5 = System.nanoTime()
  private val durationQ5 = (endQ5 - startQ5) / 1e9
  query5FullDF.show(false)
  println(f"Query 5 (Weekday Morning Drop-Offs - Full Data) executed in $durationQ5%.2f seconds")

  //with sample
  private val startQ5Sample = System.nanoTime()

  private val query5SampleDF = sampleDF
    .withColumn("hour", hour(col("dropoff_datetime")))
    .withColumn("day_of_week", expr("extract(DOW from dropoff_datetime) + 1")) // 1=Monday, ..., 7=Sunday
    .filter(col("hour").between(6, 11))
    .filter(col("day_of_week").between(1, 5))
    .filter(col("DOLocationID").isNotNull)
    .groupBy("DOLocationID", "hour")
    .agg(count("*").alias("dropoff_count"))
    .orderBy(desc("dropoff_count"))
    .limit(50)

  private val endQ5Sample = System.nanoTime()
  private val durationQ5Sample = (endQ5Sample - startQ5Sample) / 1e9
  query5SampleDF.show(false)
  println(f"Query 5 (Weekday Morning Drop-Offs - Sample Data) executed in $durationQ5Sample%.2f seconds")


  spark.stop()
}
