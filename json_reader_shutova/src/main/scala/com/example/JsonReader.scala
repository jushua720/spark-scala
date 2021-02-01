package com.example

import org.apache.log4j.{Level, Logger}


object JsonReader extends App with SparkSessionWrapper {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val json = new JsonDecoder(
    spark.sparkContext,
    JsonDecoder.Config(
      file = args(0)
    )
  )

  json.read()

  spark.stop()
}
