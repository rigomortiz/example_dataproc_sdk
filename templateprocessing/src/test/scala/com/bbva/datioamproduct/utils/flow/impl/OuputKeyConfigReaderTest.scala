package com.bbva.datioamproduct.utils.flow.impl

import com.datio.spark.test.ContextProvider
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
@RunWith(classOf[JUnitRunner])
class OuputKeyConfigReaderTest extends FlatSpec with ContextProvider with Matchers {

  private val config = ConfigFactory.defaultApplication()

  "List Config " should    "have 1 Elements " in {

    val ouputKeyConfigReader = new OuputKeyConfigReader(config)
    val list = ouputKeyConfigReader.getKeys()

    assert(list.size == 1)


  }



}
