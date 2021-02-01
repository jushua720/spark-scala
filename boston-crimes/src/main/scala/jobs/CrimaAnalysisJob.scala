package com.example
package jobs

import metrics.Metrics
import reading.{Broadcaster, CsvReader}
import transformation.{DataFrameTransformer, Transformer}
import writing.Writer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


case class JobConfig(
  readConfig: CsvReader.ReadConfig,
  broadcastConfig: Broadcaster.BroadcastConfig,
  writeConfig: Writer.WriteConfig,
  transformerClass: String)


class CrimeAnalysisJob(spark: SparkSession, config: JobConfig) {

  def run(): Unit = {
    val crimeDf = newReader()
      .read()
      .na.fill("n/a")
      .na.fill(0)

    val offenseCodes = newBroadcaster().broadcast()

    val metric = newMetricsCalculator()
    val transformer = newTransformer()

    def lookupOffenseCode(code: String): String = offenseCodes.value(code)

    val lookupOffenseCodeUdf = udf(lookupOffenseCode _)

    val crimeWithType = transformer
      .joinCrimeType(
        crimeDf,
        lookupOffenseCodeUdf,
        "OFFENSE_CODE",
        "crime_type")

    val tempAggregate = crimeWithType
      .groupBy(col("DISTRICT"))
      .agg(
        metric.totalNumber("crimes_total"),
        metric.avgCoordinate("Lat", "lat"),
        metric.avgCoordinate("Long", "lng"),
      )

    val windowSpec = Window
      .partitionBy("DISTRICT")
      .orderBy(col("crime_number").desc_nulls_last)

    val frequentCrimes = crimeWithType
      .groupBy("DISTRICT", "crime_type")
      .agg(metric.totalNumber("crime_number"))
      .transform(
        metric.frequentCrimeTypes(
          window = windowSpec,
          column = "crime_type",
          name = "frequent_crime_types")
      )

    val crimePerMonth = crimeDf
      .groupBy("DISTRICT", "YEAR", "MONTH")
      .agg(metric.totalNumber("month_total"))

    val crimesMonthly = crimePerMonth
      .groupBy(col("DISTRICT"))
      .agg(
        callUDF(
          "percentile_approx",
          col("month_total"),
          lit(0.5)
        ).as("crimes_monthly"),
      )

    val crimesAggregate = tempAggregate
      .transform(transformer.assembleStats(crimesMonthly))
      .transform(transformer.assembleStats(frequentCrimes))

    newWriter().write(crimesAggregate)
  }

  protected def newMetricsCalculator(): Metrics = new Metrics()

  protected def newReader(): CsvReader = new CsvReader(spark, config.readConfig)

  protected def newBroadcaster(): Broadcaster = new Broadcaster(spark.sparkContext, config.broadcastConfig)

  protected def newTransformer(): DataFrameTransformer = Transformer(config.transformerClass)

  protected def newWriter(): Writer = new Writer(config.writeConfig)

}