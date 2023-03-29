package com.jerin.exercise

import com.jerin.exercise.utils.{CSV, Count, Key, KeyValue, TSV, Value}
import com.jerin.exercise.utils.ExerciseUtils.{
  pathToAllFilesInDir,
  readFileFromDir
}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.Try

class OddFindOut(@transient val sparkSession: SparkSession) {
  def processFile(inputPath: String,
                  outputPath: String,
                  awsProfile: Option[String] = None): DataFrame = {

    awsProfile.foreach { profile =>
      sparkSession.sparkContext.hadoopConfiguration
        .set(
          "spark.hadoop.fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
      sparkSession.sparkContext.hadoopConfiguration
        .set("spark.hadoop.fs.s3a.aws.credentials.provider.arg", profile)
    }

    // Read the CSV and TSV files into DataFrames
    val df_with_schema_csv = readFileFromDir(
      sparkSession,
      CSV,
      KeyValue.schema,
      pathToAllFilesInDir(inputPath, CSV.format)
    )
    val df_with_schema_tsv = readFileFromDir(
      sparkSession,
      TSV,
      KeyValue.schema,
      pathToAllFilesInDir(inputPath, TSV.format)
    )

    val df_result = process(df_with_schema_csv, df_with_schema_tsv)
    // Write the result DataFrame to the output path
    df_result.write
      .option("delimiter", TSV.delimiter)
      .mode(SaveMode.Overwrite)
      .format(CSV.format)
      .save(outputPath)

    df_result
  }
  def process(df_with_schema_csv: DataFrame,
              df_with_schema_tsv: DataFrame): DataFrame = {
    // Group by the KeyValue pairs and count the occurrences
    val df_with_count_csv = df_with_schema_csv
      .groupBy(Key.name, Value.name)
      .count()
    val df_with_count_tsv = df_with_schema_tsv
      .groupBy(Key.name, Value.name)
      .count()
    // Filter for odd occurrences during the groupby operation and union the DataFrames
    val df_result = df_with_count_csv
      .union(df_with_count_tsv)
      .groupBy(Key.name, Value.name)
      .agg(sum(Count.name).alias(Count.name))
      .where(col(Count.name) % 2 =!= 0)
      .drop(Count.name)
    // Return dataframe
    df_result
  }

}
object OddFindOut extends App with Session {
  val logger = LoggerFactory.getLogger(getClass)
  Try {
    // Check argument length to make sure input and output paths are provided by the user
    if (args.length < 2) {
      throw new Exception(
        "Invalid argument list. Please provide input and output path"
      )
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val awsProfile: Option[String] = Try(args(2)).toOption
    new OddFindOut(spark).processFile(inputPath, outputPath, awsProfile)
    spark.stop()
    logger.info(
      s"Application executed successfully. Verify result in $outputPath"
    )
  }.recover {
    case ex: Throwable =>
      spark.stop()
      logger.info(s"Error running application: ${ex.getMessage}")
  }
}
