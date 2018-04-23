package com.darcemont.testeb


import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FlatSpec

class TransformationsTest extends FlatSpec with SparkTestWrapper with DataFrameSuiteBase {

  import sparkSession.implicits._

  lazy val input: DataFrame = sc.parallelize(Seq(
    ("USER_ID_1", "ITEM_ID_1", 1f, 1524490519000l), // MAX DAY, DAY 0
    ("USER_ID_2", "ITEM_ID_2", 1f, 1524375845000l), // - 1 DAY
    ("USER_ID_3", "ITEM_ID_3", 1f, 1524289445000l), // - 2 DAYS
    ("USER_ID_1", "ITEM_ID_1", 3f, 1524203045000l), // - 3 DAYS
    ("USER_ID_1", "ITEM_ID_2", 1f, 1492667045000l) // - 1 YEAR
  )).toDF("userId", "itemId", "rating", "timestamp")

  "Transformations" should "generate a users lookup" in {
    val result = Transformations.getUsersLookup(input)

    // "distinct" breaks the order so we cannot directly compare dataframes
    result.as[(String, Int)].collect().foreach { case (userIdStr: String, userIdAsInteger: Int) =>
      assert((List("USER_ID_1", "USER_ID_2", "USER_ID_3") contains userIdStr) && (userIdAsInteger >= 0 && userIdAsInteger < 3))
    }
  }

  "Transformations" should "generate a products lookup" in {
    val result = Transformations.getProductsLookup(input)

    // "distinct" breaks the order so we cannot directly compare dataframes
    result.as[(String, Int)].collect().foreach { case (itemIdStr: String, itemIdAsInteger: Int) =>
      assert((List("ITEM_ID_1", "ITEM_ID_2", "ITEM_ID_3") contains itemIdStr) && (itemIdAsInteger >= 0 && itemIdAsInteger < 3))
    }
  }

  "Transformations" should "return the max timestamp" in {
    val result = Transformations.getMaxTimestamp(input)
    assert(result == 1524490519000l)
  }

  "Transformations" should "ajust rating according to timestamp" in {
    assert(0.95 == Transformations.ajustRatingAccordingToTimestamp(1, 1524490519000l, 1524375845000l))
  }

  "Transformations" should "generate aggregate ratings" in {
    val users = sc.parallelize(Seq(
      ("USER_ID_1", 0),
      ("USER_ID_2", 1),
      ("USER_ID_3", 2)
    )).toDF("userIdStr", "userIdAsInteger")

    val products = sc.parallelize(Seq(
      ("ITEM_ID_1", 0),
      ("ITEM_ID_2", 1),
      ("ITEM_ID_3", 2)
    )).toDF("itemIdStr", "itemIdAsInteger")

    val maxTimestamp = 1524490519000l

    val expectedSchema = StructType(
      List(
        StructField("userIdAsInteger", IntegerType, nullable = false),
        StructField("itemIdAsInteger", IntegerType, nullable = false),
        StructField("ratingSum", DoubleType, nullable = true))
    )
    val expectedRdd: RDD[Row] = sc.parallelize(Seq(
      (2, 2, 0.9025),
      (1, 1, 0.95),
      (0, 0, 3.572125)
    )).map(x => Row(x._1, x._2, x._3))
    val expected = sparkSession.createDataFrame(expectedRdd, expectedSchema)

    val result = Transformations.getAggRatings(fileDf = input, usersDf = users, productsDf = products, maxTimestamp = maxTimestamp)

    assertDataFrameApproximateEquals(expected, result, 0.001)
  }
}
