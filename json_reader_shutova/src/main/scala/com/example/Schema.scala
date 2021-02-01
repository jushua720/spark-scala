package com.example

object Schema {

  case class Winemag(
    id: Option[Int],
    country: Option[String],
    points: Option[Int],
    price: Option[Double],
    title: Option[String],
    variety: Option[String],
    winery: Option[String]
  )

}
