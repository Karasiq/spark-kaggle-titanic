package com.karasiq.titanic

import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

// https://www.kaggle.com/c/titanic/details/getting-started-with-python
object GenderModel {
  private def readCsv(sqlContext: SQLContext, file: String): DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(file)
  }

  private def saveModel(ds: Dataset[(Int, Boolean)], file: String): Unit = {
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

  def run(sqlContext: SQLContext): Unit = {
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
    val genderModel = test.select($"PassengerId", $"Sex")
      .as[(Int, String)]
      .map {
        case (id, sex) ⇒
          (id, sex == "female")
      }
    saveModel(genderModel, "gendermodel.csv")

    // Second model
    import org.apache.spark.sql.functions.{avg, udf}
    val fareBin = udf((fare: Double) ⇒ math.floor(math.min(39.0, fare) / 10.0).toInt)
    def classModelTable(df: DataFrame) = {
      df
        .select($"Sex", $"Pclass", fareBin($"Fare").alias("FareBin"), $"Survived")
        .groupBy($"Sex", $"Pclass", $"FareBin")
        .agg(avg($"Survived").alias("SurvivalRate"))
    }

    val genderClassModel = {
      val survivalTable = classModelTable(train)
      survivalTable.foreach(println)
      test
        .join(survivalTable, test("Sex") === survivalTable("Sex") && test("Pclass") === survivalTable("Pclass") && fareBin(test("Fare")) === survivalTable("FareBin"))
        .select(test("PassengerId"), survivalTable("SurvivalRate") >= 0.5)
        .as[(Int, Boolean)]
    }
    saveModel(genderClassModel, "genderclassmodel.csv")
  }
}
