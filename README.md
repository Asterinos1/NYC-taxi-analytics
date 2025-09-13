# NYC Yellow Taxi Analytics

This project is a big data analysis application built using **Apache Spark** and **Scala**. It processes a multi-year dataset of NYC Yellow Taxi trip records to perform a series of analytical queries. A key goal of this project is to explore and evaluate the trade-offs between precision and performance by comparing **exact** query results with estimations derived from two approximate data summarization techniques: **Reservoir Sampling** and **Count-Min Sketch**.


##  Technologies

* **Apache Spark** 
* **Scala** 
* **Hadoop HDFS** 


##  Dataset

The dataset used is the NYC Yellow Taxi Trip Record Data for the years 2021-2023, sourced from the NYC Taxi and Limousine Commission (TLC). The dataset is approximately 9 GB in size and contains millions of trip records, including details such as pickup/drop-off times, locations, passenger counts, and fare amounts.


##  Project's Objective

The objective is centered on the implementation and comparison of two key sampling techniques:

### Reservoir Sampling (Algorithm R)

In this project, we implemented **Algorithm R**. This method is designed to select a uniform random sample of a fixed size (in this case, 100,000 trips) from a data stream of unknown size. This sample is then used to quickly approximate the results of the analytical queries, with the results scaled up to match the full dataset size.

### Count-Min Sketch

Count-Min Sketch is a probabilistic data structure that provides approximate counts for events in a data stream. The project utilizes Spark's `CountMinSketch` utility, building a distributed sketch across the dataset to provide highly accurate frequency estimations for "heavy hitters" (the most frequent items) with a very low error rate and high confidence.


##  Analytical Queries

The following six analytical queries were performed on the dataset, with results compared across exact counts, Reservoir Sampling estimates, and Count-Min Sketch estimates.

1.  **Top 20 Busiest Pickup Locations:** Identifying the most popular pickup zones.
2.  **Top 20 Weekday Morning Drop-offs:** Analyzing drop-off patterns during peak morning hours.
3.  **Busiest Taxi Routes:** Finding the most frequently traveled pickup-to-drop-off routes.
4.  **Passenger Count Distribution:** Understanding the frequency of different passenger counts per trip.
5.  **Payment Type Analysis:** Determining the distribution of payment methods (e.g., credit card, cash).
6.  **Top 10 Most Frequent Tip Amounts:** Identifying the most common tip values.


##  Evaluation & Results

The evaluation shows that while Reservoir Sampling provides a reasonable approximation, the **Count-Min Sketch** proves to be exceptionally accurate for heavy-hitter analysis. For the most frequent items, the CMS estimations were consistently either perfect or near-perfectly aligned with the exact counts. However balance between speed and precision must be considered since stricter parameters lead to slower execution of the queries for CMS.


##  How to Run

1.  **Prerequisites:** Ensure you have Apache Spark and Hadoop (with HDFS) installed and configured locally.
2.  **Dataset:** Download the NYC Yellow Taxi data for 2021-2023 and place it in your HDFS at the path specified in the code. (Note: You must modify combine the data into a single csv file)
3.  **Build:** Use `sbt` to build the project.
4.  **Run:** Execute the main application class `Main` to run the analysis. The results will be printed to the console.
