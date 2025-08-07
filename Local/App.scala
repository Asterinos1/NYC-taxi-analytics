import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.CountMinSketch

object Main extends App {

  private val spark = SparkSession.builder
    .master("local[*]")
    .appName("NYC Taxi Analyzer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN") // Reduces console noise

  // Data for 2021-2023 period (9 GB)
  private val taxiRawDataPath = "hdfs://localhost:9000/taxi_data/yellow_taxi_combined_2021_2023.csv"

  // Defining the schema to properly access the data in the CSV file
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

  // --- Data Loading and Preparation ---
  private val taxiDF = spark.read
    .option("header", "true")
    .schema(schema)
    .csv(taxiRawDataPath)
    .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    .filter(col("passenger_count") > 0)
    .filter(col("PULocationID").isNotNull && col("DOLocationID").isNotNull)
    .filter(
      col("pickup_datetime").between("2021-01-01", "2023-12-31")
    )
    .limit(35000000)
    .cache() // Cache the base DataFrame for performance

  val totalRows = taxiDF.count()
  println(s"Total rows in DataFrame: $totalRows")

  // --- Synopsis Creation ---

  // 1. Reservoir Sampling (using Spark's built-in distributed method)
  println("\n--- Creating Reservoir Sample ---")
  private val sampleFraction = 100000.0 / totalRows // approx. 100k rows
  private val sampleDF = taxiDF.sample(withReplacement = false, fraction = sampleFraction, seed = 1234L).cache()
  val sampleSize = sampleDF.count()
  println(s"Reservoir Sample size: $sampleSize rows")


  // 2. Count-Min Sketch
  println("\n--- Creating Count-Min Sketches ---")
  // Parameters for the sketch. ε (relative error), δ (confidence)
  val eps = 0.0001
  val confidence = 0.95
  val seed = 1

  // Create and populate a sketch for PULocationID
  val cmsPuLocation = CountMinSketch.create(eps, confidence, seed)
  val puLocationData = taxiDF.select("PULocationID").collect().map(_.getString(0))
  puLocationData.foreach(loc => cmsPuLocation.add(loc))
  println("Count-Min Sketch for PULocationID created.")

  // Create and populate a sketch for passenger_count
  val cmsPassengerCount = CountMinSketch.create(eps, confidence, seed)
  val passengerCountData = taxiDF.select("passenger_count").collect().map(row => row.getDouble(0).toLong)
  passengerCountData.foreach(pc => cmsPassengerCount.add(pc))
  println("Count-Min Sketch for passenger_count created.")


  // --- Helper Function for Displaying Results ---
  def runAndDisplay(queryName: String, queryFunction: () => Unit): Unit = {
    println(s"\n================== RUNNING $queryName ==================")
    val startTime = System.nanoTime()
    queryFunction()
    val duration = (System.nanoTime() - startTime) / 1e9d
    println(f"Total time for $queryName: $duration%.2f seconds")
    println(s"========================================================\n")
  }

  // =================================================================================
  // QUERY 1: Top 20 Busiest Pickup Locations (Modified from your original Query 2)
  // =================================================================================
  runAndDisplay("Query 1: Top 20 Busiest Pickup Locations", () => {
    // A. Exact Calculation
    println("--- A. Exact Calculation (Full Data) ---")
    val exactTopPuDF = taxiDF
      .groupBy("PULocationID")
      .agg(count("*").alias("exact_trip_count"))
      .orderBy(desc("exact_trip_count"))
      .limit(20)
    exactTopPuDF.show(false)

    // B. Reservoir Sample Estimate
    println("--- B. Reservoir Sample Estimate ---")
    val scalingFactor = totalRows.toDouble / sampleSize
    val sampledTopPuDF = sampleDF
      .groupBy("PULocationID")
      .agg(round(count("*") * scalingFactor).alias("sampled_trip_count"))
      .orderBy(desc("sampled_trip_count"))
      .limit(20)
    sampledTopPuDF.show(false)

    // C. Count-Min Sketch Estimate
    println("--- C. Count-Min Sketch Estimate ---")
    val topPuLocations = exactTopPuDF.select("PULocationID").collect().map(_.getString(0))
    val cmsPuEstimates = topPuLocations.map(loc => (loc, cmsPuLocation.estimateCount(loc)))
    val cmsPuDF = spark.createDataFrame(cmsPuEstimates).toDF("PULocationID", "cms_estimated_count")
    cmsPuDF.show(false)
  })

  // =================================================================================
  // QUERY 2: Top 50 Weekday Morning Dropoffs (Your original Query 5)
  // =================================================================================
  runAndDisplay("Query 2: Top 50 Weekday Morning Dropoffs", () => {
    // A. Exact Calculation
    println("--- A. Exact Calculation (Full Data) ---")
    val query5FullDF = taxiDF
      .withColumn("hour", hour(col("dropoff_datetime")))
      .withColumn("day_of_week", dayofweek(col("dropoff_datetime"))) // 1=Sun, 2=Mon...
      .filter(col("hour").between(6, 11))
      .filter(col("day_of_week").between(2, 6)) // Weekdays only
      .groupBy("DOLocationID", "hour")
      .agg(count("*").alias("exact_dropoff_count"))
      .orderBy(desc("exact_dropoff_count"))
      .limit(50)
    query5FullDF.show(50, false)

    // B. Reservoir Sample Estimate
    println("--- B. Reservoir Sample Estimate ---")
    val scalingFactor = totalRows.toDouble / sampleSize
    val query5SampleDF = sampleDF
      .withColumn("hour", hour(col("dropoff_datetime")))
      .withColumn("day_of_week", dayofweek(col("dropoff_datetime")))
      .filter(col("hour").between(6, 11))
      .filter(col("day_of_week").between(2, 6))
      .groupBy("DOLocationID", "hour")
      .agg(round(count("*") * scalingFactor).alias("sampled_dropoff_count"))
      .orderBy(desc("sampled_dropoff_count"))
      .limit(50)
    query5SampleDF.show(50, false)

    // C. Count-Min Sketch (Note: Sketching on a composite key is more complex, so we'll just show the concept on the single-key from Q1)
    println("--- C. Count-Min Sketch Estimate ---")
    println("NOTE: Count-Min is best for single keys. For this query, we'll re-use the Pickup sketch to demonstrate the principle.")
    val topDoLocations = query5FullDF.select("DOLocationID").distinct.limit(20).collect().map(_.getString(0))
    val cmsDoEstimates = topDoLocations.map(loc => (loc, cmsPuLocation.estimateCount(loc))) // Re-using PU sketch for demo
    val cmsDoDF = spark.createDataFrame(cmsDoEstimates).toDF("DOLocationID", "cms_estimated_count (approx)")
    cmsDoDF.show(false)

  })

  // =================================================================================
  // QUERY 3: Busiest Taxi Routes (Pickup-Dropoff Pair)
  // =================================================================================
  runAndDisplay("Query 3: Busiest Taxi Routes", () => {
    // A. Exact Calculation
    println("--- A. Exact Calculation (Full Data) ---")
    val busiestRoutesDF = taxiDF
      .groupBy("PULocationID", "DOLocationID")
      .agg(count("*").alias("exact_trip_count"))
      .orderBy(desc("exact_trip_count"))
      .limit(20)
    busiestRoutesDF.show(false)

    // B. Reservoir Sample Estimate
    println("--- B. Reservoir Sample Estimate ---")
    val scalingFactor = totalRows.toDouble / sampleSize
    val sampledRoutesDF = sampleDF
      .groupBy("PULocationID", "DOLocationID")
      .agg(round(count("*") * scalingFactor).alias("sampled_trip_count"))
      .orderBy(desc("sampled_trip_count"))
      .limit(20)
    sampledRoutesDF.show(false)

    // C. Count-Min Sketch (Composite key needs to be concatenated into a string)
    println("--- C. Count-Min Sketch Estimate ---")
    val cmsRoutes = CountMinSketch.create(eps, confidence, seed)
    val routeData = taxiDF.select(concat_ws("_", col("PULocationID"), col("DOLocationID")).as("route")).collect().map(_.getString(0))
    routeData.foreach(route => cmsRoutes.add(route))
    println("Count-Min Sketch for routes created.")

    val topRoutes = busiestRoutesDF.select(concat_ws("_", col("PULocationID"), col("DOLocationID")).as("route")).collect().map(_.getString(0))
    val cmsRouteEstimates = topRoutes.map(route => (route, cmsRoutes.estimateCount(route)))
    val cmsRoutesDF = spark.createDataFrame(cmsRouteEstimates).toDF("Route", "cms_estimated_count")
    cmsRoutesDF.show(false)
  })


  // =================================================================================
  // QUERY 4: Passenger Count Distribution
  // =================================================================================
  runAndDisplay("Query 4: Passenger Count Distribution", () => {
    // A. Exact Calculation
    println("--- A. Exact Calculation (Full Data) ---")
    val passengerCountDF = taxiDF
      .filter(col("passenger_count").isNotNull && col("passenger_count").between(1, 8))
      .groupBy("passenger_count")
      .agg(count("*").alias("exact_trip_count"))
      .orderBy("passenger_count")
    passengerCountDF.show()

    // B. Reservoir Sample Estimate
    println("--- B. Reservoir Sample Estimate ---")
    val scalingFactor = totalRows.toDouble / sampleSize
    val sampledPassengerCountDF = sampleDF
      .filter(col("passenger_count").isNotNull && col("passenger_count").between(1, 8))
      .groupBy("passenger_count")
      .agg(round(count("*") * scalingFactor).alias("sampled_trip_count"))
      .orderBy("passenger_count")
    sampledPassengerCountDF.show()

    // C. Count-Min Sketch Estimate
    println("--- C. Count-Min Sketch Estimate ---")
    val passengerCounts = passengerCountDF.select("passenger_count").collect().map(row => row.getDouble(0).toLong)
    val cmsPassengerEstimates = passengerCounts.map(pc => (pc, cmsPassengerCount.estimateCount(pc)))
    val cmsPassengerDF = spark.createDataFrame(cmsPassengerEstimates).toDF("passenger_count", "cms_estimated_count")
    cmsPassengerDF.show()
  })


  // =================================================================================
  // QUERY 5: Payment Type Analysis
  // =================================================================================
  runAndDisplay("Query 5: Payment Type Analysis", () => {
    // A. Exact Calculation
    println("--- A. Exact Calculation (Full Data) ---")
    // Let's create a more readable mapping for payment_type
    val paymentTypeMapping = Map("1" -> "Credit card", "2" -> "Cash", "3" -> "No charge", "4" -> "Dispute", "5" -> "Unknown", "6" -> "Voided trip")
    val toPaymentType = udf((id: String) => paymentTypeMapping.getOrElse(id, "Other"))

    val paymentTypeDF = taxiDF
      .withColumn("payment_type_desc", toPaymentType(col("payment_type")))
      .groupBy("payment_type_desc")
      .agg(count("*").alias("exact_trip_count"))
      .orderBy(desc("exact_trip_count"))
    paymentTypeDF.show(false)

    // B. Reservoir Sample Estimate
    println("--- B. Reservoir Sample Estimate ---")
    val scalingFactor = totalRows.toDouble / sampleSize
    val sampledPaymentTypeDF = sampleDF
      .withColumn("payment_type_desc", toPaymentType(col("payment_type")))
      .groupBy("payment_type_desc")
      .agg(round(count("*") * scalingFactor).alias("sampled_trip_count"))
      .orderBy(desc("sampled_trip_count"))
    sampledPaymentTypeDF.show(false)

    // C. Count-Min Sketch Estimate
    println("--- C. Count-Min Sketch Estimate ---")
    val cmsPaymentType = CountMinSketch.create(eps, confidence, seed)
    val paymentTypeData = taxiDF.select("payment_type").collect().map(_.getString(0))
    paymentTypeData.foreach(pt => cmsPaymentType.add(pt))
    println("Count-Min Sketch for payment_type created.")

    val paymentTypes = paymentTypeDF.select("payment_type_desc").collect().map(_.getString(0))
    val paymentTypeIds = paymentTypeMapping.filter { case (_, v) => paymentTypes.contains(v) }.keys.toSeq
    val cmsPaymentEstimates = paymentTypeIds.map(id => (paymentTypeMapping.getOrElse(id, "Other"), cmsPaymentType.estimateCount(id)))
    val cmsPaymentDF = spark.createDataFrame(cmsPaymentEstimates).toDF("payment_type_desc", "cms_estimated_count")
    cmsPaymentDF.show(false)
  })

  spark.stop()
}

