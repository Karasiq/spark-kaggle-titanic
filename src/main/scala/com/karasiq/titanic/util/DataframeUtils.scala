package com.karasiq.titanic.util

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

object DataframeUtils {
  private def median(column: Column) = callUDF("percentile_approx", column, lit(0.5))

  implicit class TitanicDataframeOps(private val data: DataFrame) {
    import data.sqlContext.implicits._

    def fillWithMedianValues(medianSource: DataFrame, column: String): DataFrame = {
      val medianColumn = s"Median($column)"
      val median = medianSource
        .groupBy($"Sex", $"Pclass", $"Embarked")
        .agg(DataframeUtils.median(medianSource(column)).as(medianColumn))

      data
        .join(median, data("Sex") === median("Sex") && data("Pclass") === median("Pclass") && data("Embarked") === median("Embarked"))
        .drop(median("Sex"))
        .drop(median("Pclass"))
        .drop(median("Embarked"))
        .withColumn(column, when(col(column).isNotNull, col(column)).otherwise(median(medianColumn)))
        .drop(median(medianColumn))
    }

    def fillWithMedianValues(column: String): DataFrame = {
      fillWithMedianValues(data, column)
    }
  }

  def readCsv(sqlContext: SQLContext, file: String): DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file)
      .cache()
  }

  def saveModel(df: DataFrame, file: String): Unit = {
    import df.sqlContext.implicits._
    df.select($"PassengerId", ($"Predicted".cast(DoubleType) >= 0.5).cast(IntegerType).as("Survived"))
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(file)
  }
}
