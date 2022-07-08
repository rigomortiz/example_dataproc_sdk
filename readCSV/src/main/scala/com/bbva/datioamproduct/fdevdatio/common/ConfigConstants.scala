package com.bbva.datioamproduct.fdevdatio.common

object ConfigConstants {
  val ROOT_CONFIG: String = "ReadCsv";
  val PARAMS: String = s"$ROOT_CONFIG.params"
  val MESSAGE: String = s"$PARAMS.message"
  val INPUT_CONFIG: String = s"$ROOT_CONFIG.input"
  val BIKES_CONFIG: String = s"$INPUT_CONFIG.bikes"
  val CUSTOMER_CONFIG: String = s"$INPUT_CONFIG.customer"
  val TYPE: String = s"type"
  val PATH: String = "path"
  val SCHEMA: String = s"schema.$PATH"
  val PATHS:String = s"paths"
  val DELIMITER: String = "delimiter"
  val HEADERS: String = "headers"
}
