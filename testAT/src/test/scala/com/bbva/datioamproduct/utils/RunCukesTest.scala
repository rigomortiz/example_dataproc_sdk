package com.bbva.datioamproduct.utils

import cucumber.api.CucumberOptions
import cucumber.api.junit.Cucumber
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array
  (
  ),
  glue = Array("com.datio.spark.bdt.steps", "com.bbva.datioamproduct.utils.steps"),
  plugin = Array(
    "pretty"
  ),
  tags = Array("~@ignore")
)
class RunCukesTest
