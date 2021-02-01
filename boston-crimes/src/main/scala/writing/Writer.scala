package com.example
package writing

import org.apache.spark.sql.DataFrame

object Writer {

  case class WriteConfig(outputFolder: String, format: String) {
    require(
      Seq("parquet").contains(format),
      s"Unsupported output format $format"
    )
  }
}


class Writer(config: Writer.WriteConfig) {

  def write(df: DataFrame): Unit = {
    require(df != null, "df must be specified")
    df.coalesce(1)
      .write
      .format(config.format)
      .mode("overwrite")
      .save(config.outputFolder)
  }
}


