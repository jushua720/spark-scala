package com.example

import jobs.{CrimeAnalysisJob, JobConfig}
import reading.Broadcaster.BroadcastConfig
import reading.CsvReader.ReadConfig
import writing.Writer.WriteConfig

import org.apache.log4j._


object BostonCrimesMap extends App with Context {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val job = new CrimeAnalysisJob(
    spark,
    JobConfig(
      ReadConfig(
        crimeFile = args(0),
        hasHeader = true,
        separator = ',',
      ),
      BroadcastConfig(
        offenseCodeFile = args(1),
        regex = "\\s-",
      ),
      WriteConfig(
        outputFolder = args(2),
        format = "parquet",
      ),
      transformerClass = "DataFrameTransformer",
    )
  )

  job.run()

  spark.stop()

}
