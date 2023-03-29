package com.jerin.exercise

import org.apache.spark.sql.SparkSession

trait Session {
  val spark: SparkSession = SparkSession
    .builder()
    .master("spark://localhost:7077")
    .appName("OddFindOut")
    .getOrCreate()
}
