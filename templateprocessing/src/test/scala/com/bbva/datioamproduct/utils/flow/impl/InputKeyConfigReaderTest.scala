package com.bbva.datioamproduct.utils.flow.impl

import com.datio.spark.test.ContextProvider
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class InputKeyConfigReaderTest extends FlatSpec with ContextProvider with Matchers {

  private val config = ConfigFactory.defaultApplication()

  "List Config " should    "have 2 Elements " in {

    val inputConfigReader = new InputKeyConfigReader(config)
    val list = inputConfigReader.getKeys()

    assert(list.size == 2)

  }



}
