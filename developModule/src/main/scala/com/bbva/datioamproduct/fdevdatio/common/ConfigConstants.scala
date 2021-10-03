package com.bbva.datioamproduct.fdevdatio.common

object ConfigConstants {

  val ROOT_CONFIG: String = "example_job"
  val INPUT_CONFIG: String = s"$ROOT_CONFIG.input"
  val OUTPUT_CONFIG: String = s"$ROOT_CONFIG.output"
  val PARAMS_CONFIG: String = s"$ROOT_CONFIG.params"

  val CUSTOMERS_CONFIG: String = s"$INPUT_CONFIG.t_fdev_customers"
  val CUSTOMERS_PATH: String = s"$CUSTOMERS_CONFIG.path"

  val PHONES_CONFIG: String = s"$INPUT_CONFIG.t_fdev_phones"
  val PHONES_PATH: String = s"$PHONES_CONFIG.path"

  val CUSTOMERS_PHONES_CONFIG: String = s"$OUTPUT_CONFIG.t_fdev_customersphones"
  val CUSTOMERS_PHONES_PATH: String = s"$CUSTOMERS_PHONES_CONFIG.path"
  val CUSTOMERS_PHONES_SCHEMA: String = s"$CUSTOMERS_PHONES_CONFIG.schema"

  val JWK_DATE: String = s"$PARAMS_CONFIG.jwk_date"

}
