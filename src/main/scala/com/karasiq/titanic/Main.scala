package com.karasiq.titanic

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {
  val conf = new SparkConf()
    .setAppName("Kaggle Titanic")
    .setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  GenderModel.run(sqlContext)
}
