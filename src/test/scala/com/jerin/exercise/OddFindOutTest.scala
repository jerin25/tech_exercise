package com.jerin.exercise

import com.jerin.exercise.utils.{Key, Value}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class OddFindOutTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  private val csvData = Seq((1, 1), (2, 1), (3, 1), (4, 1), (2, 1), (3, 1))
  private val tsvData =
    Seq((1, 1), (2, 1), (3, 1), (4, 1))

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("WordCount Test")
      .master("local[2]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test(
    "OddFindOut process should find odd occurrence of values for each key in all files in directory"
  ) {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val dfCsv =
      spark.sparkContext.parallelize(csvData).toDF(Key.name, Value.name)
    val dfTsv =
      spark.sparkContext.parallelize(tsvData).toDF(Key.name, Value.name)
    val expectedResult =
      Seq((2, 1), (3, 1)).toDF(Key.name, Value.name)
    val oddFindOut = new OddFindOut(spark)
    val actualOutput = oddFindOut.process(dfCsv, dfTsv)
    assert(
      actualOutput.except(expectedResult).count() == 0 && expectedResult
        .except(actualOutput)
        .count() == 0
    )
  }
  test(
    "OddFindOut processFiles read file from input directory and write file in output directory"
  ) {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val expectedResult =
      Seq((2, 7), (3, 1), (4, 7), (1, 1), (8, 7)).toDF(Key.name, Value.name)
    val oddFindOut = new OddFindOut(spark)
    val actualOutput = oddFindOut.processFile(
      "src/test/resources/input",
      "src/test/resources/output"
    )
    assert(
      actualOutput.except(expectedResult).count() == 0 && expectedResult
        .except(actualOutput)
        .count() == 0
    )
  }
}
