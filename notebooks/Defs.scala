// Databricks notebook source
// Definitions used by all notebooks.

// COMMAND ----------

// Get the login ID. We'll use this value to disambiguate paths, in case two
// people are running this stuff on the same Databricks instance. Not necessary
// on Community Edition, but not harmful, in any case.
val tags = com.databricks.logging.AttributionContext.current.tags
val Username = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
val UserHome = s"dbfs:/user/$Username"

// COMMAND ----------

val RootDir = s"$UserHome/spark-frameless"
val TweetsDataPath = s"$RootDir/tweets.parquet"
val OldTweetsDataPath = s"$RootDir/old-tweets.parquet"

// COMMAND ----------


