package com.example

import org.apache.spark.sql.SparkSession


trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .appName("json_reader")
      .getOrCreate()
  }

}