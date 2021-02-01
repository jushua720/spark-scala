package com.example
package reading


import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.io.{Codec, Source}


object Broadcaster {

  case class BroadcastConfig(offenseCodeFile: String, regex: String, separator: Char = ',')

}


class Broadcaster(sc: SparkContext, config: Broadcaster.BroadcastConfig) {

  private def load(): Map[String, String] = {
    var offenseCodes: Map[String, String] = Map()

    implicit val codec: Codec = Codec("Cp1252")
    val lines = Source.fromFile(config.offenseCodeFile)

    for (line <- lines.getLines()) {
      val fields = line.split(config.separator)
      assert(fields.length > 1, "No data to broadcast")
      offenseCodes += (fields(0) -> fields(1).split(config.regex)(0))
    }

    lines.close()
    offenseCodes
  }

  def broadcast(): Broadcast[Map[String, String]] = sc.broadcast(load())

}
