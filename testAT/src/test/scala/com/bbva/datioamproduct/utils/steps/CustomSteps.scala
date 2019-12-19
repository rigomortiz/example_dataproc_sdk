package com.bbva.datioamproduct.utils.steps

import com.datio.spark.bdt.utils.Common
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.Matchers
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.functions._

class ExampleSteps extends Matchers with ScalaDsl with EN with LazyLogging {

  Then("""^(\S+) word has exactly (\d+) matches in (\S+) dataframe$""") {
    (word: String, matches: Int, dfName: String) =>

      Common.dfMap(dfName).where(col("word").equalTo(word)).
        filter(col("count").equalTo(matches)).count shouldBe 1
  }
}
