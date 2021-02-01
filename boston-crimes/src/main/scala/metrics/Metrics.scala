package com.example
package metrics

import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

class Metrics {

  def rankCrimes(window: WindowSpec, highestRank: Int)(df: DataFrame): DataFrame = {
    df
      .withColumn("rank", dense_rank().over(window))
      .where(col("rank").leq(highestRank))
  }

  def makeString(col: String, name: String): Column = {
    concat_ws(", ", collect_list(col)).as(name)
  }

  def frequentCrimeTypes(window: WindowSpec, column: String, name: String, highestRank: Int = 3)(df: DataFrame): DataFrame = {
    require(df.columns.contains(column), "Column not found")
    df
      .transform(rankCrimes(window, highestRank))
      .groupBy("DISTRICT")
      .agg(makeString(column, name))
  }

  def totalNumber(name: String): Column = count("*").as(name)

  def avgCoordinate(column: String, name: String): Column = avg(column).as(name)

}
