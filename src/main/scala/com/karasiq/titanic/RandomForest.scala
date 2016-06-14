package com.karasiq.titanic

import com.karasiq.titanic.util.DataframeUtils._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

// https://www.kaggle.com/c/titanic/details/getting-started-with-random-forests
object RandomForest {
  def preparedData(data: DataFrame): DataFrame = {
    import data.sqlContext.implicits._

    data.fillWithNumbers("Sex")
      .fillWithNumbers("Embarked")
      .fillWithMedianValues("Age")
      .fillWithMedianValues("Embarked")
      .fillWithMedianValues("Fare")
      .withColumn("Pclass", col("Pclass") - 1)
      .withColumn("FamilySize", $"SibSp" + $"Parch" + 1)
  }

  def run(sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    val train = readCsv(sqlContext, "train.csv")

    val points = preparedData(train)
      .select($"Survived", $"Sex", $"Age", $"Pclass", $"Fare", $"FamilySize", $"Embarked")
      .as[(Int, Int, Double, Int, Double, Int, Double)]
      .map {
        case (survived, sex, age, pclass, fare, familySize, embarked) ⇒
          LabeledPoint(survived, Vectors.dense(sex, age, pclass, fare, familySize, embarked))
      }

    val categories = Map(
      0 → 2, // Sex
      2 → 3, // Pclass
      5 → 3 // Embarked
    )
    val model = DecisionTree.trainClassifier(points.rdd, 2, categories, "gini", 6, 32)
    val test = readCsv(sqlContext, "test.csv")

    val predicted = preparedData(test)
      .select($"PassengerId", $"Sex", $"Age", $"Pclass", $"Fare", $"FamilySize", $"Embarked")
      .as[(Int, Int, Double, Int, Double, Int, Double)]
      .map {
        case (passengerId, sex, age, pclass, fare, familySize, embarked) ⇒
          (passengerId, model.predict(Vectors.dense(sex, age, pclass, fare, familySize, embarked)) >= 0.5)
      }

    saveModel(predicted, "predicted.csv")
  }
}
