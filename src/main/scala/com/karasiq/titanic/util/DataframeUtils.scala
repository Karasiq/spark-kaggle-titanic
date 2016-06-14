package com.karasiq.titanic.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SQLContext}

object DataframeUtils {
  private def median(column: Column) = callUDF("percentile_approx", column, lit(0.5))

  implicit class TitanicDataframeOps(private val data: DataFrame) {
    import data.sqlContext.implicits._

    def fillWithMedianValues(column: String): DataFrame = {
      val medianColumn = s"Median($column)"
      val median = data
        .groupBy($"Sex", $"Pclass")
        .agg(DataframeUtils.median(data(column)).as(medianColumn))

      data
        .join(median, data("Sex") === median("Sex") && data("Pclass") === median("Pclass"))
        .drop(median("Sex"))
        .drop(median("Pclass"))
        .withColumn(column, when(col(column).isNotNull, col(column)).otherwise(median(medianColumn)))
        .drop(median(medianColumn))
    }

    def fillWithNumbers(column: String): DataFrame = {
      val columnIsNotEmpty = col(column).isNotNull && (col(column) !== "")
      val values = data
        .select(col(column))
        .filter(columnIsNotEmpty)
        .as[String]
        .distinct
        .collect()
      val index = udf((value: String) ⇒ values.indexOf(value))
      data.withColumn(column, when(columnIsNotEmpty, index(col(column))).otherwise(null))
    }
  }

  def readCsv(sqlContext: SQLContext, file: String): DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file)
  }

  def saveModel(ds: Dataset[(Int, Boolean)], file: String): Unit = {
    import ds.sqlContext.implicits._
    ds.map(kv ⇒ kv._1 → (if (kv._2) 1 else 0))
      .toDF()
      .withColumnRenamed("_1", "PassengerId")
      .withColumnRenamed("_2", "Survived")
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(file)
  }
}
