package com.bbva.datioamproduct.fdevdatio.common

object ConfigConstants {
  val ROOT_CONFIG: String = "ReadCsv";
  val PARAMS: String = s"$ROOT_CONFIG.params"
  val MESSAGE: String = s"$PARAMS.message"
  val INPUT_CONFIG: String = s"$ROOT_CONFIG.input"
  val OUTPUT_CONFIG: String = s"$ROOT_CONFIG.output"
  val BIKES_CONFIG: String = s"$INPUT_CONFIG.bikes"
  val CUSTOMER_CONFIG: String = s"$INPUT_CONFIG.customer"
  val CUSTOMER_BIKES_CONFIG: String = s"$OUTPUT_CONFIG.customerBikes"
  val TYPE: String = s"type"
  val PATH: String = "path"
  val SCHEMA: String = s"schema.$PATH"
  val PATHS:String = s"paths"
  val DELIMITER: String = "delimiter"
  val HEADERS: String = "headers"
  val PARTITIONS: String = "partitions"
  val MODE: String = "mode"
  val OVERWRITE: String = "overwrite"
  val APPEND: String = "append"
  val CSV: String = "csv"
  val PARQUET: String = "parquet"
  val AVRO: String = "avro"
}
