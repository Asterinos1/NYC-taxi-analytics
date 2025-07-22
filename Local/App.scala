import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random


object Main extends App {

  private val spark = SparkSession.builder
    .master("local[*]")
    .appName("NYC Taxi Analyzer")
    .getOrCreate()

  //data for 2021-2023 period. (9 gb)
  private val taxiRawDataPath = "hdfs://localhost:9000/taxi_data/yellow_taxi_combined_2021_2023.csv"

  //defining the schema to properly access the data in the csv file.
  val schema = StructType(Array(
    StructField("VendorID", StringType, true),
    StructField("tpep_pickup_datetime", StringType, true),
    StructField("tpep_dropoff_datetime", StringType, true),
    StructField("passenger_count", DoubleType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("RatecodeID", StringType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("PULocationID", StringType, true),
    StructField("DOLocationID", StringType, true),
    StructField("payment_type", StringType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("extra", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("improvement_surcharge", DoubleType, true),
    StructField("total_amount", DoubleType, true),
    StructField("congestion_surcharge", DoubleType, true),
    StructField("airport_fee", DoubleType, true)
  ))

  private val taxiDF = spark.read
    .option("header", "true")
    .schema(schema)
    .csv(taxiRawDataPath)
    .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    .withColumn("hour", hour(col("pickup_datetime")))
    .filter(col("passenger_count") > 0)  //filtering some invalid cases.
//    .filter(col("tip_amount") >= 0)

  println(s"Total rows: ${taxiDF.count()}")

  //taxiDF.limit(10).show(truncate = false)

  //Query1: Peak Hour Tipping Behavior – Compute average tips per passenger by hour.

  val start = System.nanoTime()
  private val query1DF = taxiDF
    .filter(col("passenger_count") > 0)
    .filter(col("tip_amount") >= 0)
    .withColumn("tip_per_passenger", col("tip_amount") / col("passenger_count"))
    .groupBy("hour")
    .agg(
      round(avg("tip_per_passenger"), 2).alias("avg_tip_per_passenger"),
      count("*").alias("trip_count")
    )
    .orderBy("hour")
  val end = System.nanoTime()
  private val durationSeconds = (end - start) / 1e9d
  // Show the results
  query1DF.show()
  println(f"Query 1 executed in $durationSeconds%.2f seconds")



  spark.stop()
}
