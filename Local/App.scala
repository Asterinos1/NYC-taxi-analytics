import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.sketch.CountMinSketch

object Main extends App {
  private val spark = SparkSession.builder
    .master("local[*]")
    .appName("NYC Taxi Analyzer - Fixed CMS & Sampling")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  // --- CONFIG ---
  private val taxiRawDataPath = "hdfs://localhost:9000/taxi_data/yellow_taxi_combined_2021_2023.csv"
  private val targetSampleSize = 100000L // desired reservoir sample size

  //  CMS params (epsilon, confidence)
  val eps = 0.0001
  val confidence = 0.95
  val cmsSeed = 1

  // --- SCHEMA ---
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

  // --- LOAD & PREPARE ---
  private val taxiDF = spark.read
    .option("header", "true")
    .schema(schema)
    .csv(taxiRawDataPath)
    .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    .filter(col("passenger_count") > 0)
    .filter(col("PULocationID").isNotNull && col("DOLocationID").isNotNull)
    .filter(col("pickup_datetime").between("2021-01-01", "2023-12-31"))
    .cache()

  private val totalRows = taxiDF.count()
  println(s"Total rows in DataFrame: $totalRows")

  // --- RESERVOIR SAMPLE ---
  println("\n--- Creating Reservoir Sample ---")
  val sampleFraction = math.min(1.0, targetSampleSize.toDouble / math.max(1L, totalRows).toDouble)
  val sampleDF = taxiDF.sample(withReplacement = false, fraction = sampleFraction, seed = 1234L).cache()
  val sampleSize = sampleDF.count()
  println(s"Reservoir Sample size: $sampleSize rows")

  // Safety: if sampleSize is 0 avoid division by zero
  val scalingFactor = if (sampleSize > 0) totalRows.toDouble / sampleSize.toDouble else 1.0

  // --- PAYMENT TYPE MAPPING ---
  // Using broadcast map for efficient mapping
  val paymentTypeMap = Map(
    "0.0" -> "Flex Fare trip",
    "1.0" -> "Credit card",
    "2.0" -> "Cash",
    "3.0" -> "No charge",
    "4.0" -> "Dispute",
    "5.0" -> "Unknown",
    "6.0" -> "Voided trip"
  )
  private val bPaymentMap = spark.sparkContext.broadcast(paymentTypeMap)
  private val toPaymentTypeUDF = udf((id: String) => {
    if (id == null) "Other"
    else bPaymentMap.value.getOrElse(id.trim, "Other")
  })

  // attach human readable payment_type_desc column once for reuse
  private val taxi = taxiDF.withColumn("payment_type_desc", toPaymentTypeUDF(col("payment_type"))).cache()

  // --- HELPER: Build CMS for arbitrary key expression (as string) ---
  private def buildCmsForKey(df: DataFrame, keyExpr: String): CountMinSketch = {
    df.select(expr(keyExpr).alias("_cms_key")).rdd.mapPartitions { iter =>
      val localCms = CountMinSketch.create(eps, confidence, cmsSeed)
      iter.foreach { row =>
        val v = if (row.isNullAt(0)) "__NULL__" else row.get(0).toString
        localCms.add(v)
      }
      Iterator(localCms)
    }.reduce((c1, c2) => { c1.mergeInPlace(c2); c1 })
  }

  // --- HELPER: safe estimation that returns (key, estimate) for a list of keys ---
  private def estimateForKeys(cms: CountMinSketch, keys: Seq[String]): Seq[(String, Long)] = {
    keys.map(k => (k, cms.estimateCount(if (k == null) "__NULL__" else k)))
  }

  // --- UTILITY to show DF rows and also return small collected keys ---
  private def showAndCollectKeys(df: DataFrame, keyCols: Seq[String], limit: Int = 20): Array[String] = {
    df.show(limit, false)
    df.select(keyCols.map(col): _*).limit(limit).collect().map { r =>
      keyCols.map(c => if (r.isNullAt(r.fieldIndex(c))) "__NULL__" else r.getString(r.fieldIndex(c))).mkString("_")
    }
  }

  // Timing wrapper
  private def runAndDisplay(queryName: String)(f: => Unit): Unit = {
    println(s"\n================== RUNNING $queryName ==================")
    val t0 = System.nanoTime()
    f
    val dur = (System.nanoTime() - t0) / 1e9
    println(f"Total time for $queryName: $dur%.2f seconds")
    println(s"========================================================\n")
  }

  // =================================================================================
  // QUERY 1: Top 20 Busiest Pickup Locations
  // =================================================================================
  runAndDisplay("Query 1: Top 20 Busiest Pickup Locations") {
    println("--- A. Exact Calculation (Full Data) ---")
    val exactTopPuDF = taxi.groupBy("PULocationID").agg(count("*").alias("exact_trip_count")).orderBy(desc("exact_trip_count")).limit(20)
    val topPuKeys = showAndCollectKeys(exactTopPuDF, Seq("PULocationID"), 20)

    println("--- B. Reservoir Sample Estimate ---")
    val sampledTopPuDF = sampleDF.groupBy("PULocationID").agg(round(count("*") * scalingFactor).alias("sampled_trip_count")).orderBy(desc("sampled_trip_count")).limit(20)
    sampledTopPuDF.show(false)

    println("--- C. Count-Min Sketch Estimate ---")
    // Build CMS on the raw PULocationID values (string as-is)
    val cmsPu = buildCmsForKey(taxi, "PULocationID")
    val cmsEst = estimateForKeys(cmsPu, topPuKeys)
    import spark.implicits._
    val cmsPuDF = cmsEst.toSeq.toDF("PULocationID", "cms_estimated_count")
    cmsPuDF.show(false)
  }

  // =================================================================================
  // QUERY 2: Top 50 Weekday Morning Dropoffs (DOLocationID + hour)
  // =================================================================================
  runAndDisplay("Query 2: Top 50 Weekday Morning Dropoffs") {
    println("--- A. Exact Calculation (Full Data) ---")
    val queryDF = taxi
      .withColumn("hour", hour(col("dropoff_datetime")))
      .withColumn("day_of_week", dayofweek(col("dropoff_datetime")))
      .filter(col("hour").between(6, 11))
      .filter(col("day_of_week").between(2, 6))
      .groupBy("DOLocationID", "hour")
      .agg(count("*").alias("exact_dropoff_count"))
      .orderBy(desc("exact_dropoff_count"))
      .limit(50)

    // collect composite keys for CMS queries as DOLocationID_hour
    queryDF.show(50, false)
    val topDoHourKeys = queryDF.select(concat_ws("_", col("DOLocationID"), col("hour")).alias("key")).limit(50).collect().map(_.getString(0))

    println("--- B. Reservoir Sample Estimate ---")
    val querySample = sampleDF
      .withColumn("hour", hour(col("dropoff_datetime")))
      .withColumn("day_of_week", dayofweek(col("dropoff_datetime")))
      .filter(col("hour").between(6, 11))
      .filter(col("day_of_week").between(2, 6))
      .groupBy("DOLocationID", "hour")
      .agg(round(count("*") * scalingFactor).alias("sampled_dropoff_count"))
      .orderBy(desc("sampled_dropoff_count"))
      .limit(50)
    querySample.show(50, false)

    println("--- C. Count-Min Sketch Estimate ---")
    // Build CMS over the same composite key used in the exact/grouping stage
    val cmsDo = buildCmsForKey(taxi, "concat_ws('_', DOLocationID, hour(dropoff_datetime))")
    val cmsEstimates = estimateForKeys(cmsDo, topDoHourKeys)
    cmsEstimates.toSeq.toDF("DOLocationID_hour", "cms_estimated_count").show(false)
  }

  // =================================================================================
  // QUERY 3: Busiest Taxi Routes (Pickup-Dropoff Pair)
  // =================================================================================
  runAndDisplay("Query 3: Busiest Taxi Routes") {
    println("--- A. Exact Calculation (Full Data) ---")
    val busiestRoutesDF = taxi.groupBy("PULocationID", "DOLocationID").agg(count("*").alias("exact_trip_count")).orderBy(desc("exact_trip_count")).limit(20)
    busiestRoutesDF.show(false)
    val topRouteKeys = busiestRoutesDF.select(concat_ws("_", col("PULocationID"), col("DOLocationID")).alias("route")).limit(20).collect().map(_.getString(0))

    println("--- B. Reservoir Sample Estimate ---")
    val sampledRoutesDF = sampleDF.groupBy("PULocationID", "DOLocationID").agg(round(count("*") * scalingFactor).alias("sampled_trip_count")).orderBy(desc("sampled_trip_count")).limit(20)
    sampledRoutesDF.show(false)

    println("--- C. Count-Min Sketch Estimate ---")
    // Build CMS on composite route key PULocationID_DOLocationID from raw data
    val cmsRoutes = buildCmsForKey(taxi, "concat_ws('_', PULocationID, DOLocationID)")
    val cmsRouteEst = estimateForKeys(cmsRoutes, topRouteKeys)
    cmsRouteEst.toSeq.toDF("Route", "cms_estimated_count").show(false)
  }

  // =================================================================================
  // QUERY 4: Passenger Count Distribution
  // =================================================================================
  runAndDisplay("Query 4: Passenger Count Distribution") {
    println("--- A. Exact Calculation (Full Data) ---")
    val passengerCountDF = taxi.filter(col("passenger_count").isNotNull && col("passenger_count").between(1, 8))
      .groupBy("passenger_count").agg(count("*").alias("exact_trip_count")).orderBy("passenger_count")
    passengerCountDF.show(false)

    println("--- B. Reservoir Sample Estimate ---")
    val sampledPassengerCountDF = sampleDF.filter(col("passenger_count").isNotNull && col("passenger_count").between(1, 8))
      .groupBy("passenger_count").agg(round(count("*") * scalingFactor).alias("sampled_trip_count")).orderBy("passenger_count")
    sampledPassengerCountDF.show(false)

    println("--- C. Count-Min Sketch Estimate ---")
    val passengerKeys = passengerCountDF.select("passenger_count").collect().map(r => r.getDouble(0).toInt.toString)
    val cmsPassenger = buildCmsForKey(taxi, "cast(passenger_count as string)")
    val cmsPassengerEst = estimateForKeys(cmsPassenger, passengerKeys)
    cmsPassengerEst.toSeq.toDF("passenger_count", "cms_estimated_count").show(false)
  }

  // =================================================================================
  // QUERY 5: Payment Type Analysis
  // =================================================================================
  runAndDisplay("Query 5: Payment Type Analysis") {
    println("--- A. Exact Calculation (Full Data) ---")
    val paymentTypeDF = taxi.groupBy("payment_type_desc").agg(count("*").alias("exact_trip_count")).orderBy(desc("exact_trip_count"))
    paymentTypeDF.show(false)

    println("--- B. Reservoir Sample Estimate ---")
    val sampledPaymentTypeDF = sampleDF.withColumn("payment_type_desc", toPaymentTypeUDF(col("payment_type")))
      .groupBy("payment_type_desc").agg(round(count("*") * scalingFactor).alias("sampled_trip_count")).orderBy(desc("sampled_trip_count"))
    sampledPaymentTypeDF.show(false)

    println("--- C. Count-Min Sketch Estimate ---")
    // Build CMS over the payment_type raw string (IDs as strings)
    val cmsPayment = buildCmsForKey(taxi, "coalesce(payment_type, '__NULL__')")
    val paymentKeys = paymentTypeMap.keys.toSeq
    val cmsPaymentEst = estimateForKeys(cmsPayment, paymentKeys)
    // Map back to description for printing
    val cmsPaymentDF = cmsPaymentEst.map { case (k, v) => (paymentTypeMap.getOrElse(k, "Other"), v) }.toSeq.toDF("payment_type_desc", "cms_estimated_count")
    cmsPaymentDF.show(false)
  }

  // =================================================================================
  // QUERY 6: Top 10 Most Frequent Tip Amounts
  // =================================================================================
  runAndDisplay("Query 6: Top 10 Most Frequent Tip Amounts") {
    println("--- A. Exact Calculation (Full Data) ---")
    val exactTipsDF = taxi.filter(col("tip_amount") > 0).groupBy("tip_amount").agg(count("*").alias("exact_trip_count")).orderBy(desc("exact_trip_count")).limit(10)
    exactTipsDF.show(false)

    println("--- B. Reservoir Sample Estimate ---")
    val sampledTipsDF = sampleDF.filter(col("tip_amount") > 0).groupBy("tip_amount").agg(round(count("*") * scalingFactor).alias("sampled_trip_count")).orderBy(desc("sampled_trip_count")).limit(10)
    sampledTipsDF.show(false)

    println("--- C. Count-Min Sketch Estimate ---")
    val top10Tips = exactTipsDF.select("tip_amount").collect().map(_.getDouble(0).toString)
    val cmsTip = buildCmsForKey(taxi.filter(col("tip_amount") > 0), "cast(tip_amount as string)")
    val cmsTipEst = estimateForKeys(cmsTip, top10Tips)
    cmsTipEst.toSeq.toDF("tip_amount", "cms_estimated_count").show(false)
  }

  spark.stop()
}
