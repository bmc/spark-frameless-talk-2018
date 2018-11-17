// Databricks notebook source
// MAGIC %md
// MAGIC # Introduction to Apache Spark™ with Frameless
// MAGIC 
// MAGIC Goal: To compare and contrast the native Spark DataFrames and Datasets APIs with [Frameless](https://typelevel.org/frameless/).

// COMMAND ----------

// MAGIC %md
// MAGIC ## The DataFrame API
// MAGIC 
// MAGIC Let's look at the DataFrame API first. It's arguably the most commonly used API to query data in Spark, and it's more or less consistent across Spark's supported languages.
// MAGIC 
// MAGIC We'll start by querying the tweet data.

// COMMAND ----------

// MAGIC %run ./Defs

// COMMAND ----------

import spark.implicits._
val df = spark.read.parquet(TweetsDataPath)

// COMMAND ----------

df.count() // how many tweets are in the file?

// COMMAND ----------

display(df) // what do the first 1,000 look like?

// COMMAND ----------

// MAGIC %md
// MAGIC The DataFrame API is a DSL that implements an query language. The DSL is _not_ compile-time type-safe.
// MAGIC 
// MAGIC What are the top hash tags?

// COMMAND ----------

// Count each unique tag, case-folded, and display them in order of popularity
import org.apache.spark.sql.functions._
display(
  df.cache()
    .select(explode($"hashTags").alias("tag"))
    .groupBy(lower($"tag"))
    .count
    .orderBy($"count".desc)
)

// COMMAND ----------

// MAGIC %md
// MAGIC The query language is powerful, and it's quite reminiscent of SQL. (That's deliberate.)
// MAGIC 
// MAGIC No matter what hash tags are trending, some tags always seem to be present. For instance, the polarization in our politics pretty much guarantees we can find some hits for these: 

// COMMAND ----------

val tags = df
  .filter(array_contains($"hashTags", "maga") ||
          array_contains($"hashTags", "MAGA") ||
          array_contains($"hashTags", "trump") ||
          array_contains($"hashTags", "Trump"))
  .select($"userScreenName", $"hashTags", $"text", $"timestamp", $"id")

println(tags.count())

// COMMAND ----------

display(tags)

// COMMAND ----------

df.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC But DataFrames aren't remotely compile-time type safe. This leads to several problems.
// MAGIC 
// MAGIC First, you get runtime errors (or, worse, nonsensical behavior) for type mismatches:

// COMMAND ----------

display(df.select($"timestamp" * 1000)) // This will fail

// COMMAND ----------

display(df.filter($"user" === 10)) // What does this even mean?

// COMMAND ----------

display(df.select($"user" / 100)) // This is even worse.

// COMMAND ----------

// MAGIC %md
// MAGIC You also get runtime errors when you specify non-existent columns.

// COMMAND ----------

df.select($"username", $"tweet")

// COMMAND ----------

// MAGIC %md
// MAGIC Even if you get everything right, if you pull some of the data back to the Spark driver (i.e., your application), you have to deal with Spark's `Row` type, which is akin to an `Array[Any]`.

// COMMAND ----------

val data = tags.collect()
println(data(0)(0))
println("\n\n--------")

// COMMAND ----------

val firstRowUser = data(0)(0)

// COMMAND ----------

// MAGIC %md
// MAGIC Every column in the row has type `Any`. If you need to work with the actual types, you have to cast.

// COMMAND ----------

import java.time.LocalDateTime

case class SomeTweetData(user: String, text: String, timestamp: LocalDateTime)

val goodData = data.map { row =>
  val iUser = row.fieldIndex("userScreenName") // run-time error if you get the column name wrong
  val iText = row.fieldIndex("text")
  SomeTweetData(
    user      = row.getString(iUser),
    text      = row.getString(iText),
    timestamp = row.getAs[java.sql.Timestamp]("timestamp").toLocalDateTime
  )
}

// COMMAND ----------

// MAGIC %md
// MAGIC Not surprisingly, you'll get a runtime error for a bad cast:

// COMMAND ----------

val goodData = data.map { row =>
  val iUser = row.fieldIndex("userScreenName")
  val iText = row.fieldIndex("text")
  SomeTweetData(
    user      = row.getString(iUser),
    text      = row.getString(iText),
    timestamp = row.getAs[java.sql.Date]("timestamp").toLocalDate.atStartOfDay
//                        ^^^^^^^^^^^^^
//                        Invalid cast    
  )
}

// COMMAND ----------

// MAGIC %md
// MAGIC # The Dataset API
// MAGIC 
// MAGIC One solution is the Dataset API, which allows you to cast a `Row` to a more useful type, before pulling data back to the Driver.

// COMMAND ----------

df.printSchema

// COMMAND ----------

case class Place(
  coordinates:  Array[String],
  name:         Option[String],
  placeType:    Option[String],
  fullName:     Option[String],
  countryCode:  Option[String]
)
case class TweetData(
  hashTags:       Array[String],
  text:           String,
  id:             Long,
  place:          Place,
  retweetCount:   Long,
  favoriteCount:  Long,
  user:           Option[String],
  userScreenName: Option[String],
  timestamp:      java.sql.Timestamp
)

// COMMAND ----------

val ds = df.as[TweetData] // Ugh. A cast

// COMMAND ----------

// MAGIC %md
// MAGIC How does this cast work? Spark SQL matches the DataFrame column names against the case class field names. What happens if they don't match?

// COMMAND ----------

case class PlaceBad(
  coordinates:  Array[String],
  name:         Option[String],
  place_type:   Option[String], // Not correct
  full_name:    Option[String], // Not correct
  country_code: Option[String]  // Not correct
)
case class TweetDataBad(
  hash_tags:      Array[String], // Not correct
  text:           String,
  id:             Long,
  place:          PlaceBad,
  retweetCount:   Long,
  favoriteCount:  Long,
  user:           Option[String],
  userScreenName: Option[String],
  timestamp:      java.sql.Timestamp
)

val dsBad = df.as[TweetDataBad] // Runtime error, as befits a cast operation

// COMMAND ----------

// MAGIC %md
// MAGIC You can still use a Dataset in an untyped fashion:

// COMMAND ----------

display(
  ds
    .filter(array_contains($"hashTags", "MAGA"))
    .select($"userScreenName", $"text")
    .take(3)
)

// COMMAND ----------

// MAGIC %md
// MAGIC But you can also use it in a more traditional functional style:

// COMMAND ----------

ds.filter(_.hashTags.nonEmpty).take(1).head.hashTags

// COMMAND ----------

display(
  ds.filter(_.hashTags.exists(_.toLowerCase == "maga"))
)

// COMMAND ----------

// MAGIC %md
// MAGIC Awesome! We have our types back!
// MAGIC 
// MAGIC Now, the compiler can protect us.

// COMMAND ----------

ds.filter(_.timestamp > 100)

// COMMAND ----------

ds.map(_.timestamp * 1000)

// COMMAND ----------

// MAGIC %md
// MAGIC Even better, when I collect the data back to the Driver, I no longer have to do any casting:

// COMMAND ----------

val someOfTheTags = ds.filter(_.hashTags.exists(_.toLowerCase == "maga")).collect()

// COMMAND ----------

// MAGIC %md
// MAGIC So, why not just stick with Dataset?
// MAGIC 
// MAGIC There are several problems with Datasets.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Tungsten Data Format
// MAGIC 
// MAGIC Unlike the older Spark RDD API, which looks a lot like the Scala collections API, Spark SQL does not store data partitions in memory as JVM objects. Instead, it uses a compact, optimized format called "Tungsten". This format is memory-efficient and is optimized for many of the operations Spark performs (such as sorting, which can often be done in place, without deserializing the data out of Tungsten.)

// COMMAND ----------

// MAGIC %md
// MAGIC When Spark runs a query you build up from the DataFrame API (or using SQL), it implements that query in terms of internal RDDs that can operate directly out of this compact memory format. 
// MAGIC 
// MAGIC - It does not need to generate any JVM objects for the data being read in, so GC is reduced.
// MAGIC - The memory footprint is smaller than with RDDs, because there's no object overhead.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC If you cast a DataFrame to a Dataset, _and then you invoke a functional-style lambda_, Spark has to copy the data out of Tungsten and into a JVM object, to pass it to your lambda.
// MAGIC 
// MAGIC <img src="https://github.com/bmc/spark-frameless-talk-2018/raw/master/images/tungsten-dataset.png"/>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Optimizations
// MAGIC 
// MAGIC When you stick with the DataFrame query API, Spark SQL knows exactly what you're trying to do, and it can optimize your query for you.
// MAGIC For instance, consider the following:

// COMMAND ----------

// Creating a large-ish file of user names against which to join. The join is nonsensical,
// really, but it'll serve to illustrate a point.

val MostUsersPath = s"$RootDir/most-users.parquet"
// A little more than 2 million tweets from a particular timestamp back in February, 2018
val oldTweeters = spark.read.parquet(OldTweetsDataPath)
oldTweeters
  .select($"userScreenName")
  .union(df.select($"userScreenName"))
  .distinct
  .write
  .mode("overwrite")
  .parquet(MostUsersPath)  

// COMMAND ----------

val save = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1")

val mostUsers = spark.read.parquet(MostUsersPath)
val stillAround = tags
  .join(mostUsers, mostUsers("userScreenName") === tags("userScreenName"))
  .filter(mostUsers("userScreenName") =!= "TheFukawee")
  .select(mostUsers("userScreenName"), tags("id"))

stillAround.explain

// COMMAND ----------

// MAGIC %md
// MAGIC Spark SQL's optimizer, Catalyst, will detect that your `filter` operation is inefficient: It's filtering on one side of the join _after_ the join occurs. Catalyst can rearrange the query to do the filtering first. (Or, if one side of the join is small enough, it may arrange to broadcast the entire data set to every node, something Spark calls a "broadcast join".)
// MAGIC 
// MAGIC It can do that because it understands the query.
// MAGIC 
// MAGIC That's only one of many optimizations Catalyst can do for you.
// MAGIC 
// MAGIC Now, let's do the same thing, but using a `filter` with a lambda.

// COMMAND ----------

val joined = tags
  .join(mostUsers, mostUsers("userScreenName") === tags("userScreenName"))
  .select(mostUsers("userScreenName"), tags("id"))
  .as[(String, Long)]
  .filter { t => t match {
    case (id, screenName) => screenName != "TheFukawee"
  }}

joined.explain

// COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", save)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC **Conclusion**: Datasets give you back your type-safety and your functional lambdas, but at a potential cost in performance.

// COMMAND ----------

// MAGIC %md
// MAGIC # Frameless and `TypedDataset`
// MAGIC 
// MAGIC Frameless uses the usual type programming trickery to add compile-time type safety, without compromising performance or relying on runtime casts.
// MAGIC 
// MAGIC Let's create a Frameless `TypedDataset` for our tweet stream data. We can create one directly from the Dataset (`ds`) we created above.

// COMMAND ----------

import frameless.functions.aggregate._
import frameless.syntax._
import frameless.TypedDataset
import frameless._

// COMMAND ----------

val tds = TypedDataset.create(ds)

// COMMAND ----------

// MAGIC %md
// MAGIC Oops. That didn't work.
// MAGIC 
// MAGIC Frameless uses _typed encoders_ to figure out how to convert to and from the underlying type.
// MAGIC 
// MAGIC Frameless can't infer an encoder for our case class. Likely, it's because there's no built-in encoder for one of the types we're using. 
// MAGIC 
// MAGIC Recall the [definitions](#notebook/2965424/command/2965492) of our case classes.
// MAGIC 
// MAGIC As a guess, it's probably `java.sql.Timestamp`, since everything else is either `String` or a primitive.
// MAGIC 
// MAGIC So, let's try creating an encoder, as described at <https://typelevel.org/frameless/Injection.html>.

// COMMAND ----------

implicit val timestampToLongInjection = new Injection[java.sql.Timestamp, Long] {
  def apply(ts: java.sql.Timestamp): Long = ts.getTime
  def invert(l: Long): java.sql.Timestamp = new java.sql.Timestamp(l)
}

// COMMAND ----------

val tds = TypedDataset.create(ds)

// COMMAND ----------

// MAGIC %md 
// MAGIC There's also a less verbose way to create an injection, using two lambas:

// COMMAND ----------

implicit val timestampToLongInjection = Injection((_: java.sql.Timestamp).getTime(), new java.sql.Timestamp((_: Long)))

// COMMAND ----------

// MAGIC %md
// MAGIC Use subscripting with symbols to select columns.

// COMMAND ----------

tds.select(tds('userScreenName)) // lazy: Returns another TypeDataset

// COMMAND ----------

// MAGIC %md
// MAGIC Okay, let's call an action.

// COMMAND ----------

tds
  .select(tds('userScreenName))
  .limit(10)
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC Huh. _Also_ lazy.
// MAGIC 
// MAGIC What we'd call an "action" in Spark returns a Frameless [Job](https://typelevel.org/frameless/Job.html). We have to `run()` a job.

// COMMAND ----------

tds
  .select(tds('userScreenName))
  .limit(10)
  .show()
  .run()

// COMMAND ----------

// MAGIC %md
// MAGIC More on jobs in a bit.
// MAGIC 
// MAGIC ## Type safety
// MAGIC 
// MAGIC Remember what happens when we select a bad column in a DataFrame or Dataset: A runtime error. Compare that to what happens with a `TypedDataset`.

// COMMAND ----------

ds.select('teimstamp) // Spark lets you use symbols, too

// COMMAND ----------

tds.select(tds('teimstamp))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Column operations
// MAGIC 
// MAGIC The `TypeDataset` version of `select()` supports arbitrary column operations, just like DataFrames.

// COMMAND ----------

df.select(('id * 2).as("idDoubled"), $"userScreenName")
  .show(10)

// COMMAND ----------

tds
  .select(tds('id) * 2, tds('userScreenName))
  .show(10)
  .run()

// COMMAND ----------

// MAGIC %md
// MAGIC What's with the weird column names?
// MAGIC 
// MAGIC There's no case class associated with the type of the `select()`. If you want to work with a subset of the data, while retaining better names, use a Frameless [Projection](https://typelevel.org/frameless/FeatureOverview.html#projections) or use
// MAGIC [casting](https://typelevel.org/frameless/FeatureOverview.html#casting-and-projections).

// COMMAND ----------

// Casting, where the types have to line up in order.

case class IdDoubledAndName(idDoubled: Long, userScreenName: Option[String])
val tds2 = tds
  .select(tds('id) * 2, tds('userScreenName))
  .as[IdDoubledAndName]

tds2.show(10).run()

// COMMAND ----------

// Projection, where the names are matched and order doesn't matter.

case class NamesAndIds(id: Long, userScreenName: Option[String])
val tds3 = tds.project[NamesAndIds]
tds3.show(10).run()

// COMMAND ----------

// MAGIC %md
// MAGIC In both cases, if we screw it up, we get compile-time errors.

// COMMAND ----------

// Casting, where the types aren't lined up properly.

val tds2Bad = tds
  .select(tds('userScreenName), tds('id) * 2) // wrong order
  .as[IdDoubledAndName]

// COMMAND ----------

// Projection, where the names don't match.

case class NamesAndIdsBad(id: Long, userName: Option[String])
val tds3 = tds.project[NamesAndIdsBad]

// COMMAND ----------

// Projection, where the names match, but at least one of the types is wrong.

case class NamesAndIdsBad2(id: Long, userScreenName: String)
val tds3 = tds.project[NamesAndIdsBad2]

// COMMAND ----------

// MAGIC %md
// MAGIC Again, compare to the standard Dataset cast.

// COMMAND ----------

case class NamesAndIdsBad(id: Long, userName: String)
val ds2 = df.select($"userScreenName", $"id").as[NamesAndIdsBad]

// COMMAND ----------

// MAGIC %md
// MAGIC ## Operations on columns
// MAGIC 
// MAGIC Frameless provides many of Spark's functions and transformations. If something doesn't exist, you can just call `.dataset` to drop down
// MAGIC to the Spark Dataset to call something you need.
// MAGIC 
// MAGIC Here are some examples.

// COMMAND ----------

import frameless.functions._
import frameless.functions.nonAggregate._

// COMMAND ----------

// MAGIC %md
// MAGIC Drop a column (`id`) and return a `TypedDataset` with a new tuple-based type schema.

// COMMAND ----------

tds.dropTupled('id).show(10).run()

// COMMAND ----------

// MAGIC %md 
// MAGIC Operations like `distinct` work as expected.

// COMMAND ----------

tds.select(tds('userScreenName)).distinct.show().run()

// COMMAND ----------

// MAGIC %md Aggregation is slightly less SQL-like:

// COMMAND ----------

val screenNameCounts = tds.groupBy(tds('userScreenName)).agg(count(tds('userScreenName)))
val ordered = screenNameCounts.filter(screenNameCounts('_1).isNotNone).orderBy(screenNameCounts('_2).desc)
ordered.show().run()
val topTwoScreenNames = ordered.select(ordered('_1)).take(2).run()
val firstScreenName: String = topTwoScreenNames(0).get
val secondScreenName: String = topTwoScreenNames(1).get
// Sometimes, you'll need some additional temporary variables...

// COMMAND ----------

// withColumnReplaced can be used to replace the value of a column.
val tds2 = tds.withColumnReplaced('id, tds('id) % 10000)
tds2.show(10).run()

// COMMAND ----------

tds.withColumnReplaced('id, lit(0L)).show(10).run()

// COMMAND ----------

// MAGIC %md
// MAGIC How about adding columns? There are several solutions. First, let's project our `TypedDataset` down to something more manageable.

// COMMAND ----------

case class SomeTweetData(hashTags: Array[String], text: String, userScreenName: Option[String], timestamp: java.sql.Timestamp)
val tdsSome = tds.project[SomeTweetData]
tdsSome.show(10).run()

// COMMAND ----------

// Add a column, and expand the schema (type) to a new type.
case class SomeData2(hashTags:       Array[String], 
                     text:           String, 
                     userScreenName: Option[String], 
                     timestamp:      java.sql.Timestamp,
                     id:             Long)

val tdsSome2 = tdsSome.withColumn[SomeData2](lit(0L))
tdsSome2.show(10).run()

// COMMAND ----------

// Conditionally replace a column value:
val tdsSome3 = tdsSome2
  .filter((tdsSome2('userScreenName) === Some(firstScreenName)) ||
          (tdsSome2('userScreenName) === Some(secondScreenName)))
  .withColumnTupled(
    when(tdsSome2('userScreenName) === Some(topScreenName), tdsSome2('id)).
    otherwise(lit(1L))
  )
tdsSome3.show(10).run()

// COMMAND ----------

// MAGIC %md
// MAGIC Note that the things don't always work the way you want them to, however. Try rewriting the above as follows, and it won't compile.

// COMMAND ----------

// Something more complicated
val tdsSome4 = tdsSome2
  .filter((tdsSome2('userScreenName) === Some(firstScreenName)) ||
          (tdsSome2('userScreenName) === Some(secondScreenName)))
  .withColumnTupled(
    when(tdsSome2('userScreenName) === Some(firstScreenName), lit(1L)).
    otherwise(tdsSome2('id))
  )
tdsSome4.show(10).run()

// COMMAND ----------

// Simpler way to add a column (though you will lose column names):

// Spark way:
df.select($"*", ($"id" * 2).as("idDoubled")).show(1)

// Frameless way:
tds.select(tds.asCol, tds('id) * 2).show(1).run()

// COMMAND ----------

// MAGIC %md
// MAGIC ## What about performance?
// MAGIC 
// MAGIC Let's do our previous join, but in Frameless, and check the query plan. You'll see that it's substantially similar to the
// MAGIC query plan for the corresponding DataFrame.
// MAGIC 
// MAGIC Frameless has its own DSL that "compiles" down to actual Spark calls, _at compile time_. As long as you avoid lambdas, your Frameless performance won't differ from Spark DataFrame performance.

// COMMAND ----------

case class User(userScreenName: Option[String])
case class MatchedTweet(hashTags: Array[String], text: String, timestamp: java.sql.Timestamp, id: Long, userScreenName: Option[String])
val dsMostUsers = mostUsers.as[User]
val tdsMostUsers = TypedDataset.create(dsMostUsers)
val tdsMatches = TypedDataset.create(tags.as[MatchedTweet])

val tdsJoined = tdsMatches.joinInner(tdsMostUsers) { tdsMostUsers('userScreenName) === tdsMatches('userScreenName) }
val tdsStillAround = tdsJoined
  .filter(tdsJoined.colMany('_2, 'userScreenName) =!= Some("TheFuckawee"))
  .select(tdsJoined.colMany('_2, 'userScreenName), tdsJoined.colMany('_1, 'id))
tdsStillAround.explain()

// COMMAND ----------

stillAround.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Nested schemas
// MAGIC 
// MAGIC Let's write some nested JSON data, so we can compare the DataFrame vs. Frameless approach to accessing nested columns.

// COMMAND ----------

case class Nested(tweet: TweetData, rowID: Long)
val tdsNested = tds
  .select(tds.asCol, tds('id) + 100)
  .as[Nested]
tdsNested
  .limit(100)
  .write // returns a Spark DataFrameWriter
  .mode("overwrite")
  .json(s"$RootDir/nested.json")

// COMMAND ----------

val dfNested = spark.read.option("inferSchema", "true").json(s"$RootDir/nested.json")
display(dfNested)

// COMMAND ----------

// In Spark, use "." notation.
dfNested.select($"tweet.userScreenName").show(3)

// COMMAND ----------

// Get it wrong: Runtime error.
dfNested.select($"tweet.userScreenNam").show(3)

// COMMAND ----------

// Frameless: Use colMany()
tdsNested.select(tdsNested.colMany('tweet, 'userScreenName)).show(3).run()

// COMMAND ----------

// Get it wrong: Compiler error
tdsNested.select(tdsNested.colMany('tweet, 'userScreenNam)).show(3).run()

// COMMAND ----------

// MAGIC %md
// MAGIC ## A (little) bit more on Jobs
// MAGIC 
// MAGIC Jobs compose. You can `run()` a job, or you can compose it with other jobs. (You'll see similar examples in the Frameless documentation.)

// COMMAND ----------

val job1 = tds.count()
val job2 = job1.flatMap { count => tds.take((count / 100).toInt) }
job2.run().foreach(t => println(s"@${t.userScreenName.getOrElse("?")}: ${t.text}"))

// COMMAND ----------

// Even better:

val nonRandomSamplingJob = for {
  count  <- tds.count()
  sample <- tds.take((count / 100).toInt)
} yield sample

val sample = nonRandomSamplingJob.run()
println(sample.length)

// COMMAND ----------

display(tds.dataset)

// COMMAND ----------

def sampleTweetsWithHashTags(sample: Job[Seq[TweetData]]): Job[Seq[TweetData]] = sample.map { tweets => tweets.filter(_.hashTags.nonEmpty) }

val withTags = sampleTweetsWithHashTags(nonRandomSamplingJob).run()

println("Do they match?")
println(s"Directly from sample: ${sample.filter(_.hashTags.nonEmpty).length}")
println(s"From job: ${withTags.length}")
println("-" * 64)

// COMMAND ----------

// MAGIC %md
// MAGIC # Conclusion
// MAGIC 
// MAGIC In this talk, I've only touched the surface of Frameless. There's much more it can do. For full details, see
// MAGIC <https://typelevel.org/frameless>.
// MAGIC 
// MAGIC 
// MAGIC **Q**: Which API should you use?
// MAGIC 
// MAGIC **A**: Like so much in our field, it depends.
// MAGIC 
// MAGIC With Frameless:
// MAGIC 
// MAGIC - You'll be doing more typing.
// MAGIC - You'll likely be using more temporary variables. (But what are a few temp variables between friends?)
// MAGIC - You may get some compiler errors that are confusing at first.
// MAGIC 
// MAGIC But you get strong compile-time type-safety _without_ paying the performance penalties associated with using lambdas and the Datasets API.
// MAGIC 
// MAGIC My preference is to stick with the DataFrame API when I'm just experimenting with data interactively. It's faster to type, and I don't mind the runtime errors in that use case.
// MAGIC 
// MAGIC But for production jobs, I'm beginning to prefer Frameless, for the compile-time protection.
