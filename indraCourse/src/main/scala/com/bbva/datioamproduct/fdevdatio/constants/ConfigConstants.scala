package com.bbva.datioamproduct.fdevdatio.constants

object ConfigConstants {

  val RootConfig: String = "appJob"
  val ParamsConfig: String = s"$RootConfig.params"
  val InputsConfig: String = s"$RootConfig.inputs"
  val OutputsConfig: String = s"$RootConfig.outputs"

  val FdevCustomersConfig: String = s"$InputsConfig.fdevCustomers"

  val DevNameConfig: String = "devName"
}
