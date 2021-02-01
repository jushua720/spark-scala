package com.example

import com.example.Schema.Winemag
import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, parser}
import org.apache.spark.SparkContext

object JsonDecoder {

  case class Config(file: String)

}


class JsonDecoder(sc: SparkContext, config: JsonDecoder.Config) {

  def read(): Unit = {
    val json = sc.textFile(config.file)
    implicit val winemagDecoder: Decoder[Winemag] = deriveDecoder[Winemag]

    def decode(row: String): Unit = {
      val decodeResult = parser.decode[Winemag](row)
      decodeResult match {
        case Right(winemag) => println(winemag)
        case Left(error) => throw new RuntimeException(error.getMessage)
      }
    }

    json.collect().foreach(t => decode(t))
  }

}