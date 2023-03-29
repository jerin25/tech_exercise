package com.jerin.exercise.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object ExerciseUtils {
  def readFileFromDir(sparkSession: SparkSession,
                      fileFormats: FileFormats,
                      schema: StructType,
                      inputPath: String): DataFrame = {
    sparkSession.read
      .format(CSV.format)
      .option("header", "true")
      .option("delimiter", fileFormats.delimiter)
      .schema(schema)
      .load(inputPath)
  }

  def pathToAllFilesInDir(inputPath: String, format: String): String =
    inputPath + "/*." + format
}
