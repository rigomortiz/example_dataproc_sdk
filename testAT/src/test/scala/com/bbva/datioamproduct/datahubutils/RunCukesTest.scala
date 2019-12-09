package com.bbva.datioamproduct.datahubutils

import cucumber.api.CucumberOptions
import cucumber.api.junit.Cucumber
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array
  (
  "src/test/resources/features/Example.feature"
  ),
  glue = Array("com.datio.spark.bdt.steps", "com.bbva.datioamproduct.datahubutils.steps"),
  plugin = Array(
    "pretty"
  ),
  tags = Array("~@ignore")
)
class RunCukesTest
