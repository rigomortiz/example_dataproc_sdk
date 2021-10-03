package com.bbva.datioamproduct.fdevdatio.common

object ConfigConstants {

  val ROOT_CONFIG: String = "example_job"
  val INPUT_CONFIG: String = s"$ROOT_CONFIG.input"
  val OUTPUT_CONFIG: String = s"$ROOT_CONFIG.output"
  val PARAMS_CONFIG: String = s"$ROOT_CONFIG.params"

  val CUSTOMERS_CONFIG: String = s"$INPUT_CONFIG.t_fdev_customers"
  val PHONES_CONFIG: String = s"$INPUT_CONFIG.t_fdev_phones"
  val CUSTOMERS_PHONES_CONFIG: String = s"$OUTPUT_CONFIG.t_fdev_customersphones"

  val JWK_DATE: String = s"$PARAMS_CONFIG.jwk_date"


  val PATH: String = "path"
  val TYPE: String = "type"
  val SCHEMA: String = "schema"
  val PARTITIONS: String = "partitions"
  val MODE: String = "mode"

}
