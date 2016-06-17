package com.karasiq.titanic

import com.karasiq.titanic.util.DataframeUtils._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SQLContext

// https://www.kaggle.com/c/titanic/details/getting-started-with-random-forests
object MachineLearning {
  def run(sqlContext: SQLContext): Unit = {
    val train = readCsv(sqlContext, "train.csv")
      .na.fill("C", Seq("Embarked"))
      .fillWithMedianValues("Age")
      .fillWithMedianValues("Fare")

    val test = readCsv(sqlContext, "test.csv")
      .na.fill("C", Seq("Embarked"))
      .fillWithMedianValues(train, "Age")
      .fillWithMedianValues(train, "Fare")

    val stringIndexers = Array("Survived", "Sex", "Embarked").map { field ⇒
      new StringIndexer()
        .setInputCol(field)
        .setOutputCol(s"${field}Index")
    }

    val fareDiscretizer = new QuantileDiscretizer()
      .setInputCol("Fare")
      .setOutputCol("FareIndex")
      .setNumBuckets(6)

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("SexIndex", "Pclass", "FareIndex", "EmbarkedIndex", "Parch", "SibSp", "Age"))
      .setOutputCol("Features")

    val vectorIndexer = new VectorIndexer()
      .setInputCol("Features")
      .setOutputCol("FeaturesIndexed")
      .setMaxCategories(6)

    val classifier = new RandomForestClassifier()
      .setLabelCol("SurvivedIndex")
      .setFeaturesCol("FeaturesIndexed")
      .setPredictionCol("Predicted")

    val pipeline = new Pipeline()
      .setStages(stringIndexers ++ Array(fareDiscretizer, vectorAssembler, vectorIndexer, classifier))

    val model = pipeline.fit(train)
    val predicted = model.transform(test)
    saveModel(predicted, "predicted.csv")
  }
}
