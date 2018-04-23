package com.darcemont.testeb

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Use this to test the app locally, from sbt
  * sbt "run yag.csv"
  * Output files are saved in local "files" directory
  */
object MainApp extends App {

  // input file is in args
  val inputFile = args(0)

  // file schema
  val fileSchema = StructType(
    List(
      StructField("userId", StringType, nullable = false),
      StructField("itemId", StringType, nullable = false),
      StructField("rating", FloatType, nullable = false),
      StructField("timestamp", LongType, nullable = false)
    )
  )

  // spark session, for local use !
  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("test-earlybirds")
    .config("spark.master", "local")
    .getOrCreate()

  // reading file
  val fileDf = spark.read
    .schema(fileSchema)
    .csv(inputFile)
  fileDf.cache()

  // first, the users
  // After deduplication, we use a sequence as an integer.
  val users = Transformations.getUsersLookup(fileDf)

  // then, the products
  // After deduplication, we use a sequence as an integer.
  val products = Transformations.getProductsLookup(fileDf)

  // looking for the max timestamp
  val maxTimestamp = Transformations.getMaxTimestamp(fileDf)

  // finally, we aggregate users and products
  val aggRatings = Transformations.getAggRatings(fileDf = fileDf, usersDf = users, productsDf = products, maxTimestamp = maxTimestamp)

  // save files on disk
  users.write.mode(SaveMode.Overwrite).csv("files/lookup_user.csv")
  products.write.mode(SaveMode.Overwrite).csv("files/lookup_product.csv")
  aggRatings.write.mode(SaveMode.Overwrite).csv("files/agg_ratings.csv")

  spark.stop()
}