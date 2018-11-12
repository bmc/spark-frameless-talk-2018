// Databricks notebook source
// MAGIC %md
// MAGIC # Create the data files
// MAGIC 
// MAGIC Use this notebook to create the data files we'll be using in the presentation.

// COMMAND ----------

// MAGIC %run ./Defs

// COMMAND ----------

// MAGIC %md
// MAGIC ## Create the recent tweets file
// MAGIC 
// MAGIC The following cells create a Parquet file from a Kafka stream containing recent tweets.

// COMMAND ----------

val KafkaHost = "server1.databricks.training:9092"
val StreamCheckpoint = s"$RootDir/checkpoint"

// COMMAND ----------

// MAGIC %md
// MAGIC The following cell defines the schema for the streamed JSON tweets.

// COMMAND ----------

import org.apache.spark.sql.types._

val TweetDataSchema = StructType(List(
  StructField("hashTags", ArrayType(StringType, true), true),
  StructField("text", StringType, true),
  StructField("id", LongType, true),
  StructField("createdAt", LongType, true),
  StructField("place", StructType(List(
    StructField("coordinates", ArrayType(StringType, true), true),
    StructField("name", StringType, true),
    StructField("placeType", StringType, true),
    StructField("fullName", StringType, true),
    StructField("countryCode", StringType, true))
  )),
  StructField("retweetCount", IntegerType, true),
  StructField("favoriteCount", IntegerType, true),
  StructField("user", StringType, true),
  StructField("userScreenName", StringType, true)
))

// COMMAND ----------

// MAGIC %md
// MAGIC Connect to the Kafka stream.

// COMMAND ----------

val kafkaDF = spark
  .readStream
  .option("kafka.bootstrap.servers", KafkaHost)
  .option("subscribe", "tweets")
  .format("kafka")
  .load()

// COMMAND ----------

kafkaDF.printSchema

// COMMAND ----------

// MAGIC %md 
// MAGIC We only want the tweet data, which is stored in the `value` field from the Kafka stream. That field is really JSON string data, so we can cast it and use the built-in `from_json()` Spark function to parse it.

// COMMAND ----------

import org.apache.spark.sql.functions._

val cleanDF = kafkaDF
  .select(from_json($"value".cast(StringType), TweetDataSchema).alias("message"))
  .select("message.*")
  .select($"*", ($"createdAt" / 1000).cast(TimestampType).as("timestamp"))
  .drop("createdAt")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Run the following cell if you want to examine the data before saving it. Be sure to cancel it.

// COMMAND ----------

display(cleanDF)

// COMMAND ----------

cleanDF.printSchema()

// COMMAND ----------

dbutils.fs.rm(TweetsDataPath, recurse=true)
dbutils.fs.rm(StreamCheckpoint, recurse=true)

// COMMAND ----------

val query = cleanDF
  .writeStream
  .format("parquet")
  .option("path", TweetsDataPath)
  .option("checkpointLocation", StreamCheckpoint)
  .outputMode("append")
  .start()

// COMMAND ----------

// MAGIC %md
// MAGIC Let the stream run for a few minutes. You can use the next two cells to monitor the size of the Parquet file. When the file has enough data (at least a few thousand records, but, ideally, 10,000 or more), stop the stream by running the final notebook cell.

// COMMAND ----------

display(dbutils.fs.ls(TweetsDataPath))

// COMMAND ----------

spark.read.parquet(TweetsDataPath).count

// COMMAND ----------

// MAGIC %md Run the following cell to stop the stream.

// COMMAND ----------

query.stop()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Download and ETL older tweets
// MAGIC 
// MAGIC The next cells download a JSON file of older tweets (with fewer fields) from earlier in 2018, load the JSON into DBFS, and create a Parquet from it.

// COMMAND ----------

// MAGIC %sh rm -f /tmp/old-tweets.json

// COMMAND ----------

// MAGIC %sh wget -O /tmp/old-tweets.json https://s3.amazonaws.com/ardentex-spark/old-tweets.json

// COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/old-tweets.json")

// COMMAND ----------

dbutils.fs.cp("file:/tmp/old-tweets.json", "dbfs:/tmp/old-tweets.json")

// COMMAND ----------

val uParseTimestamp = spark.udf.register("parseTimestamp", { s: String =>
  val formatter = java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME
  val dt = java.time.ZonedDateTime.parse("2018-02-02T10:03:49.000Z", formatter)
  java.sql.Timestamp.from(dt.toInstant)
})

// COMMAND ----------

val df = spark
  .read
  .option("inferSchema", "true")
  .json("dbfs:/tmp/old-tweets.json")
  .select($"*", uParseTimestamp($"timestamp").as("ts"))
  .drop("timestamp")
  .withColumnRenamed("ts", "timestamp")//.write.mode("overwrite").parquet(OldTweetsDataPath)
df
  .write
  .mode("overwrite")
  .parquet(OldTweetsDataPath)

// COMMAND ----------

val df = spark.read.parquet(OldTweetsDataPath)
display(df)

// COMMAND ----------

df.count()

// COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/old-tweets.json")

// COMMAND ----------

// MAGIC %sh rm -f /tmp/old-tweets.json

// COMMAND ----------


