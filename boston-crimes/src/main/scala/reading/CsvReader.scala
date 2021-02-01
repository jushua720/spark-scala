package com.example
package reading

import schemas.Schema.crimeSchema

import org.apache.spark.sql.{DataFrame, SparkSession}


object CsvReader {

  case class ReadConfig(crimeFile: String, hasHeader: Boolean, separator: Char)

}


class CsvReader(spark: SparkSession, config: CsvReader.ReadConfig) {

  def read(): DataFrame =
    spark.read
      .option("header", config.hasHeader.toString)
      .option("sep", config.separator.toString)
      .schema(crimeSchema)
      .csv(config.crimeFile)

}


