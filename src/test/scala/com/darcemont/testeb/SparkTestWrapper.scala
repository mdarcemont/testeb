package com.darcemont.testeb

import org.apache.spark.sql.SparkSession

trait SparkTestWrapper {

  lazy implicit val sparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("testeb-test")
      .getOrCreate()
  }

}