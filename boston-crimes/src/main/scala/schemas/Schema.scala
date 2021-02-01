package com.example
package schemas

import org.apache.spark.sql.types._

object Schema {

  val crimeSchema: StructType = StructType(Array(
    StructField("INCIDENT_NUMBER", StringType),
    StructField("OFFENSE_CODE", IntegerType),
    StructField("OFFENSE_CODE_GROUP", StringType),
    StructField("OFFENSE_DESCRIPTION", StringType),
    StructField("DISTRICT", StringType),
    StructField("REPORTING_AREA", StringType),
    StructField("SHOOTING", StringType),
    StructField("OCCURRED_ON_DATE", TimestampType),
    StructField("YEAR", StringType),
    StructField("MONTH", StringType),
    StructField("DAY_OF_WEEK", StringType),
    StructField("HOUR", IntegerType),
    StructField("UCR_PART", StringType),
    StructField("Street", StringType),
    StructField("Lat", DoubleType),
    StructField("Long", DoubleType),
    StructField("Location", StringType)
  ))

}




