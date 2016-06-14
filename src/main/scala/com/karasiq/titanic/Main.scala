package com.karasiq.titanic

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {
  val conf = new SparkConf()
    .setAppName("Kaggle Titanic")
    .setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new HiveContext(sc)

  GenderModel.run(sqlContext)
  RandomForest.run(sqlContext)
}
