package com.darcemont.testeb

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * All transformation functions
  */
object Transformations {

  /**
    * Get users lookup, from a dataframe
    */
  def getUsersLookup(df: DataFrame)(implicit s: SparkSession): DataFrame = {
    import s.implicits._
    df.select("userId").distinct().rdd.zipWithIndex().map {
      case (user, i) => (user(0).toString, i.toInt)
    }.toDF("userIdStr", "userIdAsInteger")
  }

  /**
    * Get products lookup, from a dataframe
    */
  def getProductsLookup(df: DataFrame)(implicit s: SparkSession): DataFrame = {
    import s.implicits._
    df.select("itemId").distinct().rdd.zipWithIndex().map {
      case (user, i) => (user(0).toString, i.toInt)
    }.toDF("itemIdStr", "itemIdAsInteger")
  }

  /**
    * Get max timestamp, from a dataframe
    */
  def getMaxTimestamp(df: DataFrame): Long = {
    df.select(max("timestamp")).first().getLong(0)
  }

  /**
    * Get agg ratings
    *
    * @param fileDf       main dataframe (from the input file)
    * @param usersDf      users dataframe
    * @param productsDf   products dataframe
    * @param maxTimestamp max timestamp
    */
  def getAggRatings(fileDf: DataFrame, usersDf: DataFrame, productsDf: DataFrame, maxTimestamp: Long)(implicit s: SparkSession): DataFrame = {
    import s.implicits._
    fileDf
      .join(usersDf, col("userId") === col("userIdStr"))
      .join(productsDf, col("itemId") === col("itemIdStr"))
      .select("userIdAsInteger", "itemIdAsInteger", "rating", "timestamp")
      .as[(Int, Int, Float, Long)]
      .map { case (userIdAsInteger: Int, itemIdAsInteger: Int, rating: Float, timestamp: Long) =>
        (userIdAsInteger, itemIdAsInteger, ajustRatingAccordingToTimestamp(rating, maxTimestamp, timestamp))
      }
      .toDF("userIdAsInteger", "itemIdAsInteger", "rating")
      .filter(col("rating") > 0.01)
      .groupBy("userIdAsInteger", "itemIdAsInteger")
      .sum("rating")
      .toDF("userIdAsInteger", "itemIdAsInteger", "ratingSum")
  }

  /**
    * Adjust rating according to timestamp
    *
    * @param rating
    * @param maxTimestamp
    * @param timestamp
    */
  def ajustRatingAccordingToTimestamp(rating: Float, maxTimestamp: Long, timestamp: Long): Double = {
    rating * Math.pow(0.95, TimeUnit.MILLISECONDS.toDays(maxTimestamp - timestamp))
  }

}
