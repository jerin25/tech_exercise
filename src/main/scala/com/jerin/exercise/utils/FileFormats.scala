package com.jerin.exercise.utils

sealed trait FileFormats {
  def format: String
  def delimiter: String
}
case object CSV extends FileFormats {
  val format = "csv"
  val delimiter = ","
}
case object TSV extends FileFormats {
  val format = "tsv"
  val delimiter = "\t"
}
