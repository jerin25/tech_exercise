package com.jerin.exercise.utils

import org.apache.spark.sql.types.{IntegerType, StructType}

sealed trait Schema {
  def schema: StructType
}

sealed trait fields { def name: String }

case object Key extends fields {
  val name = "Key"
}

case object Value extends fields {
  val name = "Value"
}

case object Count extends fields {
  val name = "Count"
}

case object KeyValue extends Schema {
  val schema = new StructType()
    .add("Key", IntegerType, nullable = false)
    .add("Value", IntegerType, nullable = false)

  val fieldList = Seq(Key, Value)
}
