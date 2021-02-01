package com.example

import org.apache.spark.sql.SparkSession

trait Context {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("BostonCrimes")
    .getOrCreate()

}
