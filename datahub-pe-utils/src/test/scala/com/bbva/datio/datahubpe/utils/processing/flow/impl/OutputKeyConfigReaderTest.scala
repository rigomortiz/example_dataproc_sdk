package com.bbva.datio.datahubpe.utils.processing.flow.impl

import com.datio.spark.test.ContextProvider
import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class OutputKeyConfigReaderTest extends FlatSpec with ContextProvider with Matchers {

  private val config = ConfigFactory.defaultApplication()

  "List Config " should "have 1 Elements " in {

    val outputKeyConfigReader = new OutputKeyConfigReader(config)
    val list = outputKeyConfigReader.getKeys()
    assert(list.size == 1)

  }


}
