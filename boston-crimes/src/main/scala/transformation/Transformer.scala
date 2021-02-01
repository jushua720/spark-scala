package com.example
package transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col


object Transformer {

  def apply(transformerClass: String): DataFrameTransformer = {
    getClass
      .getClassLoader
      .loadClass(s"com.example.transformation.$transformerClass")
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[DataFrameTransformer]
  }

}


class DataFrameTransformer {

  def joinCrimeType(
      df: DataFrame,
      func: UserDefinedFunction,
      columnToJoin: String,
      colName: String)
    : DataFrame = df.withColumn(colName, func(col(columnToJoin)))

  def assembleStats(joinDf: DataFrame, column: String = "DISTRICT")(df: DataFrame): DataFrame =
    df
      .join(joinDf, df(column) === joinDf(column))
      .drop(joinDf(column))

}
