package com.karasiq.titanic

import com.karasiq.titanic.util.DataframeUtils._
import org.apache.spark.sql.{DataFrame, SQLContext}

// https://www.kaggle.com/c/titanic/details/getting-started-with-python
object GenderModel {
  def run(sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions.{avg, udf}
    import sqlContext.implicits._

    val train = readCsv(sqlContext, "train.csv")
    val test = readCsv(sqlContext, "test.csv")

    def survivorsProportion(df: DataFrame): Double = {
      def survived(df: DataFrame): Long = df.filter($"Survived" === 1).count()
      survived(df).toDouble / df.count()
    }

    val menOnlyStats = train.filter($"Sex" === "male")
    val womenOnlyStats = train.filter($"Sex" !== "male")

    println(s"Proportion of men who survived is ${survivorsProportion(menOnlyStats)}")
    println(s"Proportion of women who survived is ${survivorsProportion(womenOnlyStats)}")

    // First model
    val genderModel = test.withColumn("Predicted", $"Sex" === "female")
    saveModel(genderModel, "gendermodel.csv")

    // Second model
    val fareBin = udf((fare: Double) ⇒ math.floor(math.min(39.0, fare) / 10.0).toInt)
    def classModelTable(df: DataFrame) = {
      df
        .groupBy($"Sex", $"Pclass", fareBin($"Fare").as("FareBin"))
        .agg(avg($"Survived").alias("SurvivalRate"))
    }

    val genderClassModel = {
      val survivalTable = classModelTable(train)
      test
        .join(survivalTable, test("Sex") === survivalTable("Sex") && test("Pclass") === survivalTable("Pclass") && fareBin(test("Fare")) === survivalTable("FareBin"))
        .withColumn("Predicted", survivalTable("SurvivalRate") >= 0.5)
    }
    saveModel(genderClassModel, "genderclassmodel.csv")
  }
}
